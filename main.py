"""Forward new Tieba threads with author names and text previews to QQ groups."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import suppress
from copy import deepcopy
from html import unescape

import aiohttp

from astrbot.api import AstrBotConfig
from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star

# Legacy clients receive generated display names instead of current nicknames.
CLIENT_VERSION = "22.0.0.0"


class TiebaPlugin(Star):
    """Poll one forum and persist pending deliveries for each group."""

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        """Initialize plugin resources.

        Args:
            context: AstrBot plugin context.
            config: Settings from the plugin configuration page.
        """
        super().__init__(context)
        self.config = config
        self.session: aiohttp.ClientSession | None = None
        self.task: asyncio.Task | None = None

    async def initialize(self) -> None:
        """Validate configuration, restore state, and start the polling worker.

        Raises:
            ValueError: The polling interval or a group ID is invalid.
        """
        self.forum = str(self.config.get("forum_name", "")).strip()
        self.interval = int(self.config.get("poll_interval_seconds", 60))
        if self.interval < 10:
            raise ValueError("poll_interval_seconds must be at least 10")
        self.groups = list(
            dict.fromkeys(
                str(group).strip() for group in self.config.get("group_ids", [])
            )
        )
        if any(
            not group.isascii() or not group.isdigit() or int(group) < 1
            for group in self.groups
        ):
            raise ValueError("group_ids must contain positive numeric QQ group IDs")
        self.platform_id = str(self.config.get("platform_id", "")).strip()
        self.state = await self.get_kv_data("state", {}) or {}
        if self.state.get("forum") != self.forum:
            self.state = {"forum": self.forum, "cursor": None, "pending": []}
        if not self.forum or not self.groups:
            self.logger.warning(
                "Configure forum_name and group_ids to start Tieba polling"
            )
        self.session = aiohttp.ClientSession(
            headers={"User-Agent": f"bdtb for Android {CLIENT_VERSION}"},
            timeout=aiohttp.ClientTimeout(total=20),
            trust_env=True,
        )
        self.task = asyncio.create_task(self._run(), name="tieba-poll-worker")

    async def terminate(self) -> None:
        """Stop the worker and release the HTTP session on plugin unload."""
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError):
                await self.task
        if self.session is not None:
            await self.session.close()

    async def _run(self) -> None:
        """Check immediately on startup, then wait between polling cycles."""
        while True:
            if self.config.get("enabled", True) and self.forum and self.groups:
                try:
                    await self._poll()
                except Exception:
                    self.logger.exception("Tieba polling cycle failed")
            await asyncio.sleep(self.interval)

    async def _fetch_threads(self) -> list[dict]:
        """Fetch creation-sorted pages until the previous cursor is reached.

        Returns:
            Threads newer than the cursor, sorted from oldest to newest.
            On the initial poll, returns the first page for establishing a baseline.

        Raises:
            RuntimeError: Tieba rejects the request or returns an invalid listing.
            aiohttp.ClientError: An HTTP request fails.
            asyncio.TimeoutError: An HTTP request times out.
        """
        if self.session is None:
            raise RuntimeError("Plugin HTTP session is not initialized")
        cursor = self.state["cursor"]
        threads = {}
        page = 1
        while True:
            params = {
                "_client_type": "2",
                "_client_version": CLIENT_VERSION,
                "kw": self.forum,
                "pn": str(page),
                # The first page is capped; a larger rn would skip threads on page 2.
                "rn": "10",
                "sort_type": "1",
            }
            signature = "".join(f"{key}={params[key]}" for key in sorted(params))
            params["sign"] = (
                hashlib.md5((signature + "tiebaclient!!!").encode("utf-8"))
                .hexdigest()
                .upper()
            )
            async with self.session.post(
                "https://c.tieba.baidu.com/c/f/frs/page", data=params
            ) as response:
                response.raise_for_status()
                payload = await response.json(content_type=None)
            if not isinstance(payload, dict) or str(payload.get("error_code")) != "0":
                code = payload.get("error_code") if isinstance(payload, dict) else None
                raise RuntimeError(f"Tieba listing failed with error code {code}")
            items = payload.get("thread_list")
            if (
                not isinstance(items, list)
                or not isinstance(payload.get("forum"), dict)
                or str(payload.get("sort_type")) != "1"
            ):
                raise RuntimeError("Tieba returned an invalid creation-sorted listing")
            users = {
                str(user.get("id")): user
                for user in payload.get("user_list") or []
                if isinstance(user, dict)
            }
            reached_cursor = False
            valid_count = 0
            for item in items:
                if not isinstance(item, dict):
                    continue
                # Real threads have tid; advertisements may have only an id.
                tid = str(item.get("tid", ""))
                if not tid.isascii() or not tid.isdigit() or int(tid) < 1:
                    continue
                try:
                    created_at = int(item.get("create_time", 0))
                except (ValueError, TypeError):
                    continue
                if created_at <= 0:
                    continue
                valid_count += 1
                if cursor is not None and (created_at, int(tid)) <= tuple(cursor):
                    # Pinned historical threads do not delimit the latest-post pages.
                    if str(item.get("is_top", 0)) != "1":
                        reached_cursor = True
                    continue
                title = unescape(str(item.get("title") or tid)).strip()
                author = {
                    **users.get(str(item.get("author_id")), {}),
                    **(item.get("author") or {}),
                }
                author_name = (
                    unescape(
                        str(author.get("name_show") or author.get("name") or "未知用户")
                    ).strip()
                    or "未知用户"
                )
                content = unescape(
                    "".join(
                        str(fragment.get("text") or "")
                        for fragment in item.get("abstract") or []
                        if isinstance(fragment, dict)
                        and str(fragment.get("type", 0)) in {"0", "1", "4"}
                    )
                ).strip()
                threads[tid] = {
                    "id": tid,
                    "title": title,
                    "created_at": created_at,
                    "author": author_name,
                    "content_preview": (
                        content[:10] + "······"
                        if len(content) >= 10
                        else content or "（无文字内容）"
                    ),
                }
            if items and not valid_count:
                raise RuntimeError("Tieba listing contains no valid thread timestamps")
            if (
                cursor is None
                or reached_cursor
                or not items
                or str((payload.get("page") or {}).get("has_more", 0)) != "1"
            ):
                break
            page += 1
            await asyncio.sleep(0.5)
        return sorted(
            threads.values(), key=lambda item: (item["created_at"], int(item["id"]))
        )

    async def _poll(self) -> None:
        """Queue newly discovered threads and retry each group's pending messages.

        Raises:
            RuntimeError: The configured platform is absent or ambiguous.
        """
        platforms = [
            platform
            for platform in self.context.platform_manager.platform_insts
            if platform.meta().name == "aiocqhttp"
            and (not self.platform_id or platform.meta().id == self.platform_id)
        ]
        if len(platforms) != 1:
            raise RuntimeError(
                "Configure platform_id to select exactly one aiocqhttp adapter"
            )
        platform_id = platforms[0].meta().id
        groups = [f"{platform_id}:GroupMessage:{group}" for group in self.groups]
        state = deepcopy(self.state)
        try:
            threads = await self._fetch_threads()
        except Exception:
            self.logger.exception(
                "Tieba fetch failed; pending deliveries will still be retried"
            )
        else:
            if threads:
                latest = threads[-1]
                if state["cursor"] is not None:
                    for thread in threads:
                        state["pending"].append({**thread, "groups": list(groups)})
                else:
                    self.logger.info(
                        "Established Tieba baseline for forum %s", self.forum
                    )
                state["cursor"] = [latest["created_at"], int(latest["id"])]
            elif state["cursor"] is None:
                # An empty forum has no historical threads to suppress.
                state["cursor"] = [0, 0]
        # Save discoveries before sending, so a restart can resume pending groups.
        for thread in state["pending"]:
            thread["groups"] = [group for group in thread["groups"] if group in groups]
        state["pending"] = [thread for thread in state["pending"] if thread["groups"]]
        await self.put_kv_data("state", state)
        self.state = state
        blocked_groups = set()
        for thread in list(state["pending"]):
            message = MessageChain(
                chain=[
                    Plain(
                        f"【{self.forum}吧 · 新帖】\n{thread['title']}\n"
                        f"发帖人：{thread.get('author', '未知用户')}\n"
                        f"内容：{thread.get('content_preview', '（暂无摘要）')}\n"
                        f"https://tieba.baidu.com/p/{thread['id']}"
                    )
                ]
            )
            for group in list(thread["groups"]):
                if group in blocked_groups:
                    continue
                try:
                    sent = await asyncio.wait_for(
                        self.context.send_message(group, message), timeout=30
                    )
                    if not sent:
                        raise RuntimeError("No matching platform is available")
                except Exception:
                    blocked_groups.add(group)
                    self.logger.exception("Tieba delivery failed for group %s", group)
                    continue
                thread["groups"].remove(group)
                if not thread["groups"]:
                    state["pending"].remove(thread)
                await self.put_kv_data("state", state)
                self.logger.info("Delivered Tieba thread %s to %s", thread["id"], group)
                await asyncio.sleep(1)
