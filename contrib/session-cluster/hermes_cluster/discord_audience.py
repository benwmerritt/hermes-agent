"""Conservative Discord visibility labels, refreshed from the platform before delivery."""
from __future__ import annotations

import hashlib

import discord

from .ledger import OwnershipError, canonical


def channel_allowed(channel, config: dict) -> bool:
    guild = getattr(channel, "guild", None)
    if guild is None or str(guild.id) != str(config["guild_id"]):
        return False
    candidates = {str(channel.id), str(getattr(channel, "parent_id", ""))}
    return bool(candidates & set(map(str, config["allowed_channel_ids"])))


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
