"""Conservative Discord visibility labels, refreshed from the platform before delivery."""
from __future__ import annotations

import hashlib

import discord

from .ledger import OwnershipError, canonical


def normalize_discord_scope(config: dict) -> dict:
    """Preserve legacy grants; broad channel access requires an explicit opt-in."""
    result = dict(config)
    guilds = config.get("allowed_guild_ids", [config.get("guild_id")])
    all_channels = config.get("all_channels", False)
    if not isinstance(all_channels, bool):
        raise ValueError("all_channels must be a boolean")
    result["all_channels"] = all_channels
    for name, values in (("allowed_guild_ids", guilds),
                         ("allowed_user_ids", config.get("allowed_user_ids")),
                         ("allowed_channel_ids", config.get("allowed_channel_ids", []))):
        if not isinstance(values, list):
            raise ValueError(f"{name} must be a list of Discord snowflakes")
        values = [str(value) for value in values]
        if any(not value.isascii() or not value.isdigit() for value in values):
            raise ValueError(f"{name} must contain explicit Discord snowflakes")
        if not values and (name != "allowed_channel_ids" or not all_channels):
            raise ValueError(f"{name} must not be empty")
        result[name] = values
    if all_channels and result["allowed_channel_ids"]:
        raise ValueError("all_channels cannot be combined with a channel allowlist")
    return result


def destination_allowed(guild_id, channel_id, parent_id, config: dict) -> bool:
    guilds = config.get("allowed_guild_ids", [config.get("guild_id")])
    if guild_id is None or str(guild_id) not in set(map(str, guilds)):
        return False
    return config.get("all_channels") is True or bool(
        {str(channel_id), str(parent_id)} & set(map(str, config["allowed_channel_ids"])))


def channel_allowed(channel, config: dict) -> bool:
    guild = getattr(channel, "guild", None)
    return destination_allowed(getattr(guild, "id", None), channel.id,
                               getattr(channel, "parent_id", None), config)


def _acl(channel):
    rows = []
    for target, overwrite in channel.overwrites.items():
        allow, deny = overwrite.pair()
        rows.append((str(target.id), type(target).__name__, allow.value, deny.value))
    return sorted(rows)


async def audience_for(client, source, config: dict) -> str:
    channel = await client.fetch_channel(int(source.chat_id))
    if not channel_allowed(channel, config) or str(getattr(channel.guild, "id", "")) != str(source.scope_id):
        raise OwnershipError("Discord destination is outside the configured guild and channels")
    parent_id = getattr(channel, "parent_id", None)
    is_thread = isinstance(channel, discord.Thread)
    if is_thread and (str(channel.id) != str(source.thread_id) or str(parent_id) != str(source.parent_chat_id)):
        raise OwnershipError("Discord thread parent changed or does not match the conversation")
    parent = await client.fetch_channel(int(parent_id)) if is_thread else channel
    guild = channel.guild
    roles = await guild.fetch_roles()
    everyone = next((role for role in roles if role.id == guild.id), None)
    if everyone is None:
        raise OwnershipError("Discord visibility could not be established")
    restricted = any(overwrite.view_channel is False for overwrite in parent.overwrites.values())
    private_thread = is_thread and channel.type == discord.ChannelType.private_thread
    if parent.permissions_for(everyone).view_channel and not restricted and not private_thread:
        return f"public:{guild.id}"

    members = []
    actor_visible = False
    for user_id in sorted(map(str, config["allowed_user_ids"])):
        try:
            member = await guild.fetch_member(int(user_id))
        except discord.NotFound:
            members.append((user_id, "absent"))
            continue
        visible = bool(channel.permissions_for(member).view_channel)
        members.append((user_id, sorted(str(role.id) for role in member.roles), visible))
        if user_id == str(source.user_id):
            actor_visible = visible
    if not actor_visible:
        raise OwnershipError("conversation owner no longer has Discord channel visibility")
    thread_members = []
    if private_thread:
        thread_members = sorted(str(member.id) for member in await channel.fetch_members())
        if str(source.user_id) not in thread_members:
            raise OwnershipError("conversation owner is not a member of the private thread")
    signature = [str(guild.id), str(channel.id), str(parent.id), _acl(parent), members, thread_members,
                 sorted((str(role.id), role.permissions.view_channel, role.permissions.administrator) for role in roles)]
    digest = hashlib.sha256(canonical(signature).encode()).hexdigest()[:32]
    return f"private:{guild.id}:{channel.id}:{digest}"
