"""Native Discord behavior with fake platform I/O and a real ownership ledger."""
from types import SimpleNamespace
import re
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from gateway.session import SessionSource, build_session_key
from hermes_cluster.discord_connector import DiscordConnector, PromptView
from hermes_cluster.ledger import Ledger, OwnershipError
from hermes_cluster.media import MediaStore


def fixture(tmp_path):
    ledger = Ledger(tmp_path / "ledger")
    media = MediaStore(tmp_path / "media", ledger)
    route, controls = AsyncMock(), AsyncMock()
    config = {"guild_id": "1", "bot_id": "2", "allowed_user_ids": ["3"], "allowed_channel_ids": ["10"]}
    connector = DiscordConnector(config, route, controls, ledger=ledger, media=media, relay_url="http://connector.test")
    everyone = MagicMock(spec=discord.Role)
    everyone.id, everyone.permissions = 1, discord.Permissions(view_channel=True)
    actor = MagicMock(spec=discord.Member)
    actor.id, actor.bot, actor.display_name, actor.roles = 3, False, "Owner", [everyone]
    guild = SimpleNamespace(id=1, name="Guild", fetch_roles=AsyncMock(return_value=[everyone]),
                            fetch_member=AsyncMock(return_value=actor))
    parent = MagicMock(spec=discord.TextChannel)
    parent.id, parent.guild, parent.overwrites, parent.name, parent.topic = 10, guild, {}, "parent", None
    parent.permissions_for.return_value = discord.Permissions(view_channel=True)
    thread = MagicMock(spec=discord.Thread)
    thread.id, thread.parent_id, thread.parent, thread.guild = 11, 10, parent, guild
    thread.name, thread.type = "Topic", discord.ChannelType.public_thread
    thread.permissions_for.return_value = discord.Permissions(view_channel=True)
    thread.fetch_members = AsyncMock(return_value=[SimpleNamespace(id=3)])
    thread.send = AsyncMock(return_value=SimpleNamespace(id=100, attachments=[SimpleNamespace(url="https://cdn.discordapp.com/test.txt")]))
    message = SimpleNamespace(edit=AsyncMock(), add_reaction=AsyncMock(), remove_reaction=AsyncMock())
    thread.get_partial_message.return_value = message
    client = SimpleNamespace(user=SimpleNamespace(id=2),
        get_channel=lambda ident: {10: parent, 11: thread}.get(ident),
        fetch_channel=AsyncMock(side_effect=lambda ident: {10: parent, 11: thread}[ident]))
    connector.adapter._client = client
    connector.adapter._allowed_user_ids = {"3"}
    source = SessionSource(platform=connector.adapter.platform, chat_id="11", chat_type="thread", thread_id="11",
                           parent_chat_id="10", scope_id="1", user_id="3", user_name="Owner")
    return connector, source, parent, thread, actor, everyone


@pytest.mark.asyncio
async def test_native_slash_routing_scoped_sends_media_and_persistent_prompt(tmp_path):
    connector, source, parent, thread, actor, everyone = fixture(tmp_path)
    interaction = SimpleNamespace(channel=thread, channel_id=11, guild_id=1, guild=thread.guild,
                                  user=actor, id=90, response=SimpleNamespace(send_message=AsyncMock(), defer=AsyncMock()),
                                  followup=SimpleNamespace(send=AsyncMock()))
    event = connector.adapter._build_slash_event(interaction, "/stop")
    assert build_session_key(event.source) == build_session_key(source)
    await connector.adapter.handle_message(event)
    connector.route.assert_awaited_once_with(event)
    foreign = SimpleNamespace(channel=thread, author=SimpleNamespace(id=99, bot=False))
    assert connector.adapter._discord_message_admission(foreign, claim=True) == (False, False)
    interaction.user = foreign.author
    assert not await connector.adapter._check_slash_authorization(interaction, "/thread")
    interaction.response.send_message.assert_awaited_once()
    interaction.user = actor

    audience = await connector.audience(source)
    owner, _ = connector.ledger.admit(agent_id="timmy", native_key=build_session_key(source), source=source.to_dict(),
        audience=audience, event_id="initial", payload={"text": "test"}, actor="3")
    connector.ledger.transition(owner["id"], 1, "ready")
    owner = connector.ledger.get(owner["id"])
    sent = await connector.dispatch(owner, {"op": "send", "chat_id": "11", "content": "reply"})
    assert sent["success"] and connector.ledger.owns_message("100", owner["id"], bot_only=True)
    with pytest.raises(OwnershipError):
        await connector.dispatch(owner, {"op": "edit", "chat_id": "11", "message_id": "999", "content": "foreign"})
    with pytest.raises(OwnershipError):
        await connector.dispatch(owner, {"op": "send", "chat_id": "11", "content": "foreign", "metadata": {"thread_id": "12"}})
    ident = connector.media.put(owner["id"], b"fixture", "test.txt", "text/plain")
    result = await connector.dispatch(owner, {"op": "send_media", "chat_id": "11", "source_url": f"http://connector.test/relay/media/{ident}"})
    assert result["success"]
    file = thread.send.call_args.kwargs["files"][0]
    assert file.filename == "test.txt"
    with pytest.raises(OwnershipError):
        await connector.dispatch(owner, {"op": "send_media", "chat_id": "11", "source_url": "http://169.254.169.254/latest"})

    result = await connector.dispatch(owner, {"op": "prompt", "chat_id": "11", "prompt_id": "worker.1234",
        "content": "Approve fixture?", "options": [{"id": "once", "label": "Allow once"}, {"id": "deny", "label": "Deny"}]})
    assert result["success"]
    # Reconstruct a persistent View from the durable ledger, as after connector restart.
    prompt = connector.ledger.pending_prompts()[0]
    view = PromptView(connector, prompt["id"], prompt["options"])
    interaction.message = SimpleNamespace(id=100)
    await view.children[0].callback(interaction)
    connector.prompt_response.assert_awaited_once_with("worker.1234", "once", interaction)
    interaction.message = SimpleNamespace(id=999)
    await view.children[0].callback(interaction)
    assert connector.prompt_response.await_count == 1


@pytest.mark.asyncio
async def test_visibility_changes_stop_delivery_and_private_membership_changes_label(tmp_path):
    connector, source, parent, thread, actor, everyone = fixture(tmp_path)
    public = await connector.audience(source)
    assert public == "public:1"
    owner, _ = connector.ledger.admit(agent_id="timmy", native_key=build_session_key(source), source=source.to_dict(),
        audience=public, event_id="initial", payload={"text": "test"}, actor="3")
    parent.overwrites = {everyone: discord.PermissionOverwrite(view_channel=False)}
    private = await connector.audience(source)
    assert private != public and private.startswith("private:1:11:")
    with pytest.raises(OwnershipError, match="visibility changed"):
        await connector.notice(owner, "private content")
    thread.send.assert_not_awaited()
    thread.type = discord.ChannelType.private_thread
    before = await connector.audience(source)
    thread.fetch_members.return_value = [SimpleNamespace(id=3), SimpleNamespace(id=4)]
    assert await connector.audience(source) != before
    thread.fetch_members.return_value = [SimpleNamespace(id=4)]
    with pytest.raises(OwnershipError, match="not a member"):
        await connector.audience(source)


@pytest.mark.asyncio
async def test_relay_final_edit_keeps_the_complete_discord_reply(tmp_path):
    from gateway.config import PlatformConfig
    from gateway.relay.adapter import RelayAdapter
    from gateway.relay.descriptor import CapabilityDescriptor
    connector, source, parent, thread, actor, everyone = fixture(tmp_path)
    owner, _ = connector.ledger.admit(agent_id="timmy", native_key=build_session_key(source), source=source.to_dict(),
        audience=await connector.audience(source), event_id="initial", payload={"text": "test"}, actor="3")
    connector.ledger.remember_message("100", owner["id"], 1, "bot")
    class Bridge:
        async def send_outbound(self, action, *, platform=None):
            return await connector.dispatch(owner, action)
    descriptor = CapabilityDescriptor(contract_version=1, platform="discord", label="Discord",
        max_message_length=2000, supports_draft_streaming=False, supports_edit=True,
        supports_threads=True, markdown_dialect="discord", len_unit="chars")
    relay = RelayAdapter(PlatformConfig(), descriptor, Bridge())
    words = [f"token{i:04d}" for i in range(350)]
    text = " ".join(words)
    result = await relay.edit_message("11", "100", text, finalize=True)
    assert result.success
    initial = thread.get_partial_message.return_value.edit.call_args.kwargs["content"]
    continuation = "".join(call.kwargs["content"] for call in thread.send.await_args_list)
    assert re.findall(r"token\d{4}", initial + continuation) == words
