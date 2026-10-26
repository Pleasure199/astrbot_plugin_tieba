"""Exercise polling and delivery contracts without starting AstrBot or sending QQ messages."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def plugin_module():
    """Load the plugin with minimal AstrBot API doubles.

    Returns:
        The plugin module using the real aiohttp package.
    """

    class Star:
        """Provide the context and logger supplied by the plugin loader."""

        def __init__(self, context):
            self.context = context
            self.logger = logging.getLogger("tieba-test")

    modules = {
        name: ModuleType(name)
        for name in (
            "astrbot",
            "astrbot.api",
            "astrbot.api.star",
            "astrbot.api.event",
            "astrbot.api.message_components",
        )
    }
    modules["astrbot.api"].AstrBotConfig = dict
    modules["astrbot.api.star"].Star = Star
    modules["astrbot.api.star"].Context = SimpleNamespace
    modules["astrbot.api.event"].MessageChain = SimpleNamespace
    modules["astrbot.api.message_components"].Plain = lambda text: SimpleNamespace(
        text=text
    )
    spec = importlib.util.spec_from_file_location(
        "tieba_plugin_test", Path(__file__).resolve().parents[1] / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


@pytest.fixture
def plugin(plugin_module):
    """Build a worker with isolated platform and persistence doubles.

    Args:
        plugin_module: Loaded plugin module.

    Returns:
        A plugin instance and its durable state snapshots.
    """
    platform = SimpleNamespace(meta=lambda: SimpleNamespace(name="aiocqhttp", id="qq"))
    context = SimpleNamespace(
        platform_manager=SimpleNamespace(platform_insts=[platform]),
        send_message=AsyncMock(return_value=True),
    )
    instance = plugin_module.TiebaPlugin(context, {})
    instance.forum = "python"
    instance.interval = 60
    instance.groups = ["10001", "10002"]
    instance.platform_id = ""
    instance.state = {"forum": "python", "cursor": None, "pending": []}
    instance.session = MagicMock()
    instance.session.close = AsyncMock()
    snapshots = []
    instance.put_kv_data = AsyncMock(
        side_effect=lambda key, value: snapshots.append(deepcopy(value))
    )
    instance.get_kv_data = AsyncMock(return_value={})
    return instance, snapshots


def thread(tid, created_at, top=0):
    """Build a representative Tieba thread.

    Args:
        tid: Numeric thread identifier.
        created_at: Creation timestamp, independent of reply time.
        top: Whether the thread is pinned.

    Returns:
        A thread listing record.
    """
    return {
        "id": tid,
        "tid": tid,
        "create_time": created_at,
        "last_time_int": 9999,
        "is_top": top,
        "title": f"Thread &amp; {tid}",
    }


def listing(*threads, more=0):
    """Build a successful creation-sorted listing.

    Args:
        threads: Thread records for this page.
        more: Whether another page is available.

    Returns:
        A JSON response payload.
    """
    return {
        "error_code": "0",
        "sort_type": 1,
        "forum": {"name": "python"},
        "thread_list": list(threads),
        "page": {"has_more": more},
    }


def responses(instance, *payloads):
    """Supply ordered HTTP responses without network access.

    Args:
        instance: Plugin instance whose session is mocked.
        payloads: JSON response payloads.
    """
    contexts = []
    for payload in payloads:
        response = SimpleNamespace(
            raise_for_status=MagicMock(), json=AsyncMock(return_value=payload)
        )
        context = MagicMock()
        context.__aenter__ = AsyncMock(return_value=response)
        contexts.append(context)
    instance.session.post.side_effect = contexts


@pytest.mark.asyncio
async def test_fetch_baseline_decodes_titles_and_uses_signed_small_pages(plugin):
    instance, _ = plugin
    responses(instance, listing(thread(30, 300), thread(20, 200), more=1))
    result = await instance._fetch_threads()
    assert [item["id"] for item in result] == ["20", "30"]
    assert result[-1]["title"] == "Thread & 30"
    assert instance.session.post.call_count == 1
    params = instance.session.post.call_args.kwargs["data"]
    assert params["rn"] == "10"
    assert params["sort_type"] == "1"
    assert len(params["sign"]) == 32


@pytest.mark.asyncio
async def test_author_uses_current_nickname_with_modern_client_version(plugin):
    instance, _ = plugin
    post = {
        **thread(11140279156, 300),
        "author_id": 17596389861565,
        "author": {},
    }
    payload = listing(post)
    payload["user_list"] = [
        {
            "id": 17596389861565,
            "name": "Pleasant_3",
            "name_show": "Pleasant_",
        }
    ]
    responses(instance, payload)
    result = await instance._fetch_threads()
    # The old 9.7.8.0 client returns a generated name for this same author.
    assert (
        instance.session.post.call_args.kwargs["data"]["_client_version"] == "22.0.0.0"
    )
    assert result[0]["author"] == "Pleasant_"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("content", "preview"),
    [
        ("", "（无文字内容）"),
        ("一二三四五六七八九", "一二三四五六七八九"),
        ("一二三四五六七八九十", "一二三四五六七八九十······"),
        ("一二三四五六七八九十甲", "一二三四五六七八九十······"),
    ],
)
async def test_messages_include_author_and_preview_at_length_boundary(
    plugin, monkeypatch, content, preview
):
    instance, snapshots = plugin
    instance.state["cursor"] = [100, 10]
    instance.groups = ["10001"]
    post = {
        **thread(20, 200),
        "author_id": 77,
        "author": {"show_icon_list": []},
        "abstract": [{"type": 0, "text": content}],
    }
    payload = listing(post, thread(10, 100))
    payload["user_list"] = [{"id": 77, "name_show": "发帖昵称", "name": "login"}]
    responses(instance, payload)
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await instance._poll()
    queued = snapshots[0]["pending"][0]
    assert queued["author"] == "发帖昵称"
    assert queued["content_preview"] == preview
    message = instance.context.send_message.await_args.args[1].chain[0].text
    assert message == (
        "【python吧 · 新帖】\nThread & 20\n发帖人：发帖昵称\n"
        f"内容：{preview}\nhttps://tieba.baidu.com/p/20"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("author", [{"name_show": "昵称"}, {"name": "昵称"}])
async def test_preview_combines_text_fragments_and_excludes_image_urls(plugin, author):
    instance, _ = plugin
    post = {
        **thread(20, 200),
        "author": author,
        "abstract": [
            {"type": 0, "text": " A&amp;"},
            {"type": "1", "text": "B"},
            {"type": 3, "text": "https://example.com/image.jpg"},
            {"type": 4, "text": "@好友 "},
        ],
    }
    responses(instance, listing(post))
    result = await instance._fetch_threads()
    assert result[0]["author"] == "昵称"
    assert result[0]["content_preview"] == "A&B@好友"


@pytest.mark.asyncio
async def test_fetch_paginates_past_pinned_history_and_deduplicates(
    plugin, monkeypatch
):
    instance, _ = plugin
    instance.state["cursor"] = [100, 10]
    responses(
        instance,
        listing(thread(1, 1, top="1"), thread(30, 300), more=1),
        listing(thread(30, 300), thread(21, 200), thread(20, 200), more=1),
        listing(thread(10, 100), thread(2, 2), more=1),
    )
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    result = await instance._fetch_threads()
    assert [item["id"] for item in result] == ["20", "21", "30"]
    assert instance.session.post.call_count == 3
    assert [
        call.kwargs["data"]["pn"] for call in instance.session.post.call_args_list
    ] == ["1", "2", "3"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        {"error_code": 110001},
        {**listing(thread(1, 1)), "sort_type": 0},
        {**listing(thread(1, 1)), "thread_list": None},
        listing(thread(1, "invalid")),
        listing(thread(1, 0)),
    ],
)
async def test_invalid_responses_do_not_establish_baseline(plugin, payload):
    instance, _ = plugin
    responses(instance, payload)
    with pytest.raises(RuntimeError):
        await instance._fetch_threads()
    assert instance.state["cursor"] is None


@pytest.mark.asyncio
async def test_old_bumped_threads_and_ads_are_not_new_posts(plugin):
    instance, _ = plugin
    instance.state["cursor"] = [200, 20]
    responses(instance, listing(thread(1, 1), {"id": 999, "create_time": 9999}))
    assert await instance._fetch_threads() == []


@pytest.mark.asyncio
async def test_first_poll_sets_baseline_without_sending_history(plugin):
    instance, snapshots = plugin
    responses(instance, listing(thread(30, 300), thread(20, 200)))
    await instance._poll()
    assert snapshots[-1]["cursor"] == [300, 30]
    assert snapshots[-1]["pending"] == []
    instance.context.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_forum_can_deliver_its_first_thread(plugin, monkeypatch):
    instance, _ = plugin
    responses(instance, listing(), listing(thread(10, 100)))
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await instance._poll()
    assert instance.state["cursor"] == [0, 0]
    await instance._poll()
    assert instance.context.send_message.await_count == 2


@pytest.mark.asyncio
async def test_per_group_failures_resume_after_restart_without_resending_successes(
    plugin, plugin_module, monkeypatch
):
    instance, snapshots = plugin
    instance.state["cursor"] = [100, 10]
    responses(instance, listing(thread(30, 300), thread(20, 200), thread(10, 100)))
    instance.context.send_message.side_effect = [RuntimeError("offline"), True, True]
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await instance._poll()
    assert len(snapshots[0]["pending"]) == 2
    assert snapshots[0]["pending"][0]["groups"] == [
        "qq:GroupMessage:10001",
        "qq:GroupMessage:10002",
    ]
    assert [call.args[0] for call in instance.context.send_message.await_args_list] == [
        "qq:GroupMessage:10001",
        "qq:GroupMessage:10002",
        "qq:GroupMessage:10002",
    ]
    durable_state = deepcopy(snapshots[-1])
    assert [item["id"] for item in durable_state["pending"]] == ["20", "30"]
    assert all(
        item["groups"] == ["qq:GroupMessage:10001"] for item in durable_state["pending"]
    )
    restarted = plugin_module.TiebaPlugin(instance.context, {})
    for attribute in (
        "forum",
        "interval",
        "groups",
        "platform_id",
        "put_kv_data",
        "session",
    ):
        setattr(restarted, attribute, getattr(instance, attribute))
    restarted.state = durable_state
    restarted.context.send_message.reset_mock(side_effect=True)
    restarted.context.send_message.return_value = True
    responses(restarted, listing(thread(30, 300)))
    await restarted._poll()
    assert restarted.state["pending"] == []
    assert [
        call.args[0] for call in restarted.context.send_message.await_args_list
    ] == ["qq:GroupMessage:10001", "qq:GroupMessage:10001"]
    assert (
        "/p/20"
        in restarted.context.send_message.await_args_list[0].args[1].chain[0].text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, RuntimeError("network")])
async def test_pending_delivery_retries_when_fetch_fails_or_platform_disappears(
    plugin, monkeypatch, failure
):
    instance, _ = plugin
    instance.state["cursor"] = [100, 10]
    instance.state["pending"] = [
        {
            "id": "20",
            "title": "Saved post",
            "created_at": 200,
            "groups": ["qq:GroupMessage:10001"],
        }
    ]
    instance._fetch_threads = AsyncMock(
        side_effect=RuntimeError("verification required")
    )
    instance.context.send_message.side_effect = [failure, True]
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await instance._poll()
    assert len(instance.state["pending"]) == 1
    await instance._poll()
    assert instance.state["pending"] == []
    assert instance.state["cursor"] == [100, 10]


@pytest.mark.asyncio
async def test_persistence_failure_does_not_send_or_advance_in_memory(plugin):
    instance, _ = plugin
    instance.state["cursor"] = [100, 10]
    responses(instance, listing(thread(20, 200), thread(10, 100)))
    instance.put_kv_data.side_effect = OSError("database unavailable")
    with pytest.raises(OSError):
        await instance._poll()
    assert instance.state["cursor"] == [100, 10]
    instance.context.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_removed_groups_are_pruned_and_new_groups_get_no_backlog(
    plugin, monkeypatch
):
    instance, _ = plugin
    instance.state["cursor"] = [100, 10]
    instance.state["pending"] = [
        {
            "id": "20",
            "title": "Saved post",
            "created_at": 200,
            "groups": ["qq:GroupMessage:10001"],
        }
    ]
    instance.groups = ["10002"]
    responses(instance, listing(thread(10, 100)))
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    await instance._poll()
    assert instance.state["pending"] == []
    instance.context.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_multiple_adapters_require_explicit_platform_id(plugin):
    instance, _ = plugin
    instance.context.platform_manager.platform_insts.append(
        SimpleNamespace(meta=lambda: SimpleNamespace(name="aiocqhttp", id="other"))
    )
    with pytest.raises(RuntimeError, match="platform_id"):
        await instance._poll()
    instance.session.post.assert_not_called()
    instance.platform_id = "other"
    responses(instance, listing())
    await instance._poll()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "config", [{"poll_interval_seconds": 1}, {"group_ids": ["bad"]}]
)
async def test_invalid_configuration_fails_before_starting_worker(plugin, config):
    instance, _ = plugin
    instance.config = config
    with pytest.raises(ValueError):
        await instance.initialize()
    assert instance.task is None


@pytest.mark.asyncio
async def test_initialize_resets_state_when_forum_changes_and_terminate_closes_session(
    plugin, plugin_module, monkeypatch
):
    instance, _ = plugin
    instance.config = {"forum_name": "new forum", "group_ids": ["10001"]}
    instance.get_kv_data.return_value = {
        "forum": "old forum",
        "cursor": [100, 10],
        "pending": [],
    }
    instance._run = AsyncMock()
    session = instance.session
    session_factory = MagicMock(return_value=session)
    monkeypatch.setattr(plugin_module.aiohttp, "ClientSession", session_factory)
    await instance.initialize()
    assert session_factory.call_args.kwargs["headers"]["User-Agent"] == (
        "bdtb for Android 22.0.0.0"
    )
    assert instance.state == {"forum": "new forum", "cursor": None, "pending": []}
    await instance.terminate()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_disabled_worker_never_polls(plugin, monkeypatch):
    instance, _ = plugin
    instance.config = {"enabled": False}
    instance._poll = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await instance._run()
    instance._poll.assert_not_awaited()
