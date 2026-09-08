"""One native Discord socket forwarding conversations to retained Relay workers."""
from __future__ import annotations

import os
import re
from urllib.parse import urlsplit

import discord

from gateway.config import PlatformConfig
from gateway.session import SessionSource
from plugins.platforms.discord.adapter import DiscordAdapter

from .discord_audience import audience_for, channel_allowed
from .ledger import OwnershipError


class RoutingDiscordAdapter(DiscordAdapter):
    def __init__(self, connector):
        self.connector = connector
        config = connector.config
        super().__init__(PlatformConfig(token=os.environ.get("DISCORD_BOT_TOKEN", ""), enabled=True,
            extra={"allowed_users": config["allowed_user_ids"], "allowed_channels": config["allowed_channel_ids"],
                   "missed_message_backfill": {"enabled": False}}))
        self._text_batch_delay_seconds = 0
        self._text_batch_split_delay_seconds = 0
        self._snapshot_gate_env()

    def _snapshot_gate_env(self):
        # Never inherit broad allowlists or other bot identities from a coordinator's shell.
        self._gate_env_snapshot = {
            "DISCORD_ALLOWED_USERS": ",".join(self.connector.config["allowed_user_ids"]),
            "DISCORD_ALLOWED_CHANNELS": ",".join(self.connector.config["allowed_channel_ids"]),
            "DISCORD_ALLOW_BOTS": "none", "DISCORD_ALLOW_ALL_USERS": "false",
            "GATEWAY_ALLOW_ALL_USERS": "false", "DISCORD_ALLOWED_ROLES": "",
        }

    def _gate_env(self, name, default=""):
        return self._gate_env_snapshot.get(name, default) or default

    def _discord_message_admission(self, message, *, claim):
        if not self.connector.raw_allowed(message.channel, message.author):
            return False, False
        return super()._discord_message_admission(message, claim=claim)

    async def _check_slash_authorization(self, interaction, command_text):
        if not self.connector.raw_allowed(interaction.channel, interaction.user):
            await interaction.response.send_message("This bot is not enabled for this user or channel.", ephemeral=True)
            return False
        return await super()._check_slash_authorization(interaction, command_text)

    def _missed_message_backfill_enabled(self):
        return False

    def _discord_history_backfill(self):
        return False

    async def handle_message(self, event):
        if not self.connector.source_allowed(event.source):
            raise OwnershipError("normalized Discord source is outside the connector grant")
        await self.connector.route(event)


class PromptView(discord.ui.View):
    def __init__(self, connector, prompt_id: str, options: list):
        super().__init__(timeout=None)
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,32}", prompt_id):
            raise ValueError("invalid prompt identifier")
        if not options or len(options) > 25:
            raise ValueError("a Discord prompt requires 1 to 25 options")
        styles = {"primary": discord.ButtonStyle.primary, "danger": discord.ButtonStyle.danger}
        seen = set()
        for option in options:
            option_id = str(option["id"])
            custom_id = f"hc:{prompt_id}:{option_id}"
            if not option_id or option_id in seen or len(custom_id.encode()) > 100:
                raise ValueError("invalid or oversized Discord prompt identifier")
            seen.add(option_id)
            button = discord.ui.Button(label=str(option["label"])[:80] or "Choose",
                                       style=styles.get(option.get("style"), discord.ButtonStyle.secondary),
                                       custom_id=custom_id)
            async def selected(interaction, selected_id=option_id):
                await connector.select_prompt(prompt_id, selected_id, interaction)
            button.callback = selected
            self.add_item(button)


class DiscordConnector:
    def __init__(self, config: dict, route, prompt_response, *, ledger, media, relay_url: str):
        self.config = dict(config)
        for name in ("guild_id", "bot_id"):
            self.config[name] = str(config[name])
            if not self.config[name].isdigit():
                raise ValueError(f"{name} must be a Discord snowflake")
        for name in ("allowed_user_ids", "allowed_channel_ids"):
            self.config[name] = list(map(str, config[name]))
            if not self.config[name] or any(not value.isdigit() for value in self.config[name]):
                raise ValueError(f"{name} must contain explicit Discord snowflakes")
        self.route, self.prompt_response = route, prompt_response
        self.ledger, self.media = ledger, media
        parsed = urlsplit(relay_url)
        scheme = {"ws": "http", "wss": "https"}.get(parsed.scheme, parsed.scheme)
        self.media_base = f"{scheme}://{parsed.netloc}"
        self.adapter = RoutingDiscordAdapter(self)

    @property
    def connected(self):
        return self.adapter.is_connected

    async def connect(self):
        if not await self.adapter.connect():
            return False
        if str(self.adapter._client.user.id) != self.config["bot_id"]:
            await self.adapter.disconnect()
            raise OwnershipError("connected Discord identity does not match the configured bot")
        for prompt in self.ledger.pending_prompts():
            if prompt.get("message_id"):
                self.adapter._client.add_view(PromptView(self, prompt["id"], prompt["options"]),
                                              message_id=int(prompt["message_id"]))
        return True

    async def disconnect(self):
        await self.adapter.disconnect()

    async def select_prompt(self, prompt_id, option_id, interaction):
        if not self.raw_allowed(interaction.channel, interaction.user):
            await interaction.response.send_message("This control is not available to this user or channel.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            prompt = next((row for row in self.ledger.pending_prompts() if row["id"] == prompt_id), None)
            if (prompt is None or str(interaction.user.id) != prompt["actor"]
                    or str(interaction.channel_id) != prompt["channel_id"]
                    or str(getattr(interaction.message, "id", "")) != prompt["message_id"]):
                raise OwnershipError("This control is expired or belongs to another conversation.")
            owner = self.ledger.get(prompt["conversation_id"])
            await self._authorize_action(owner, {"chat_id": prompt["channel_id"]})
            await self.prompt_response(prompt_id, option_id, interaction)
        except OwnershipError as exc:
            await interaction.followup.send(str(exc), ephemeral=True)
            return
        except Exception:
            await interaction.followup.send("The decision could not be confirmed. Check worker status before retrying.", ephemeral=True)
            return
        await interaction.followup.send("Decision received and saved for this worker.", ephemeral=True)

    def raw_allowed(self, channel, actor) -> bool:
        return bool(channel is not None and actor is not None and not getattr(actor, "bot", False)
                    and str(actor.id) in self.config["allowed_user_ids"] and channel_allowed(channel, self.config))

    def source_allowed(self, source) -> bool:
        return bool(str(source.platform.value) == "discord" and str(source.scope_id) == self.config["guild_id"]
                    and str(source.user_id) in self.config["allowed_user_ids"]
                    and {str(source.chat_id), str(source.parent_chat_id)} & set(self.config["allowed_channel_ids"]))

    async def audience(self, event_or_source):
        source = getattr(event_or_source, "source", event_or_source)
        if isinstance(source, dict):
            source = SessionSource.from_dict(source)
        if not self.source_allowed(source):
            raise OwnershipError("Discord source is outside the configured grant")
        return await audience_for(self.adapter._client, source, self.config)

    async def _authorize_action(self, owner, action):
        current = self.ledger.get(owner["id"])
        if current is None or current["generation"] != owner["generation"] or current["source"] != owner["source"]:
            raise OwnershipError("outbound action belongs to a stale conversation owner")
        source = SessionSource.from_dict(owner["source"])
        chat_id = str(action.get("chat_id", ""))
        if chat_id != str(source.chat_id):
            raise OwnershipError("worker cannot send to another conversation")
        metadata = dict(action.get("metadata") or {})
        if metadata.get("thread_id") and str(metadata["thread_id"]) != str(source.thread_id or source.chat_id):
            raise OwnershipError("worker cannot redirect an outbound action to another thread")
        if await self.audience(source) != owner["audience"]:
            raise OwnershipError("Discord visibility changed; conversation requires reconciliation")
        reply_to = action.get("reply_to")
        if reply_to and not self.ledger.owns_message(str(reply_to), owner["id"]):
            raise OwnershipError("reply target belongs to another conversation")
        # Never let forwarded metadata select webhooks, alternate identities, or channels.
        safe_metadata = {"thread_id": source.thread_id} if source.thread_id else {}
        return source, safe_metadata

    def _remember(self, owner, result):
        if isinstance(result, dict):
            return result
        ids = [result.message_id] if result.message_id else []
        raw = result.raw_response if isinstance(result.raw_response, dict) else {}
        ids.extend(raw.get("message_ids") or [])
        ids.extend(raw.get("continuation_message_ids") or [])
        ids.extend(getattr(result, "continuation_message_ids", ()) or ())
        for message_id in dict.fromkeys(str(value) for value in ids if value):
            self.ledger.remember_message(message_id, owner["id"], owner["generation"], "bot")
        return {"success": result.success, "message_id": result.message_id, "error": result.error}

    async def notice(self, owner, text):
        if self.ledger.get(owner["id"]) is None:
            # Admission errors have no worker yet. Authorize the original native
            # source without inventing a writable owner in the ledger.
            source = SessionSource.from_dict(owner["source"])
            if owner["generation"] != 0 or await self.audience(source) != owner["audience"]:
                raise OwnershipError("unadmitted notice has no authorized destination")
            result = await self.adapter.send(source.chat_id, text,
                metadata={"thread_id": source.thread_id} if source.thread_id else None)
            return self._remember(owner, result)
        return await self.dispatch(owner, {"op": "send", "chat_id": owner["source"]["chat_id"], "content": text})

    async def dispatch(self, owner: dict, action: dict) -> dict:
        source, metadata = await self._authorize_action(owner, action)
        operations = {"send": self._send, "edit": self._edit, "typing": self._typing,
                      "get_chat_info": self._chat_info, "react": self._react,
                      "send_media": self._send_media, "prompt": self._prompt,
                      "thread_rename": self._rename_thread}
        handler = operations.get(action.get("op"))
        if handler is None:
            return {"success": False, "error": "operation is not supported by this connector"}
        return await handler(owner, action, source, metadata)

    async def _send(self, owner, action, source, metadata):
        result = await self.adapter.send(source.chat_id, str(action.get("content", "")),
                                         reply_to=action.get("reply_to"), metadata=metadata)
        return self._remember(owner, result)

    async def _edit(self, owner, action, source, metadata):
        message_id = str(action.get("message_id", ""))
        if not self.ledger.owns_message(message_id, owner["id"], bot_only=True):
            raise OwnershipError("worker cannot edit a foreign or user-authored message")
        result = await self.adapter.edit_message(source.chat_id, message_id, str(action.get("content", "")),
                                                finalize=bool(action.get("finalize", False)), metadata=metadata)
        return self._remember(owner, result)

    async def _typing(self, owner, action, source, metadata):
        await self.adapter.send_typing(source.chat_id, metadata=metadata)
        return {"success": True}

    async def _chat_info(self, owner, action, source, metadata):
        return await self.adapter.get_chat_info(source.chat_id)

    async def _react(self, owner, action, source, metadata):
        message_id = str(action.get("message_id", ""))
        if not self.ledger.owns_message(message_id, owner["id"]):
            raise OwnershipError("reaction target belongs to another conversation")
        channel = await self.adapter._resolve_channel(source.chat_id)
        message = channel.get_partial_message(int(message_id))
        emoji = str(action.get("emoji", ""))
        if not emoji or len(emoji) > 100:
            raise ValueError("invalid reaction")
        if action.get("remove"):
            await message.remove_reaction(emoji, self.adapter._client.user)
        else:
            await message.add_reaction(emoji)
        return {"success": True}

    async def _send_media(self, owner, action, source, metadata):
        url = str(action.get("source_url", ""))
        prefix = self.media_base + "/relay/media/"
        if not url.startswith(prefix) or any(value in url[len(prefix):] for value in ("/", "?", "#", "%")):
            raise OwnershipError("only this connector's authenticated media references are accepted")
        path, stored = self.media.get(url[len(prefix):], owner["id"])
        result = await self.adapter.send_document(source.chat_id, str(path),
            caption=str(action.get("content", "")) or None, file_name=stored["filename"], metadata=metadata)
        return self._remember(owner, result)

    async def _rename_thread(self, owner, action, source, metadata):
        if not source.thread_id or str(action.get("message_id", "")) != source.thread_id:
            raise OwnershipError("worker can rename only its own thread")
        expected_name = action.get("only_if_current_name")
        if action.get("only_if_connector_created"):
            if not source.auto_thread_created or not source.auto_thread_initial_name:
                raise OwnershipError("thread was not created by this connector")
            expected_name = source.auto_thread_initial_name
        result = await self.adapter.rename_thread(source.thread_id, str(action.get("thread_name", "")),
                                                 only_if_current_name=expected_name)
        return {"success": bool(result)}

    async def _prompt(self, owner, action, source, metadata):
        prompt_id = str(action.get("prompt_id", ""))
        options = action.get("options") or []
        view = PromptView(self, prompt_id, options)
        self.ledger.create_prompt(prompt_id, owner, source.chat_id, options, int(action.get("timeout_s", 300)))
        content = str(action.get("content", ""))
        if len(content) > 2000:
            preliminary = await self._send(owner, {"content": content}, source, metadata)
            if not preliminary.get("success"):
                return preliminary
            content = "Choose an option for the request above."
        channel = await self.adapter._resolve_channel(source.chat_id)
        message = await channel.send(content=content or "Choose an option.", view=view, allowed_mentions=discord.AllowedMentions.none())
        self.ledger.attach_prompt(prompt_id, str(message.id))
        self.ledger.remember_message(str(message.id), owner["id"], owner["generation"], "bot")
        return {"success": True, "message_id": str(message.id)}
