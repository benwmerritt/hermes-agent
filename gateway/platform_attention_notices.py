"""One home-channel notice when a platform needs an operator and the gateway stopped retrying it.

A non-retryable runtime fatal (WhatsApp unlinked the device, so only a re-pair helps) drops the
platform from the reconnect queue; without a notice that is a log line nobody reads while scheduled
sends fail for weeks. The failed platform cannot carry its own notice, so it goes to the owning
profile's OTHER home channels through the same send path the state.db and cron store warnings use.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Callable


async def send_platform_attention_notice(runner, *, platform, profile_home, render_message: Callable[[], str]) -> int:
    """Post ``render_message()`` to every live home channel of the profile at *profile_home* except
    *platform*'s own; returns how many accepted it. The message is rendered inside the owning
    profile's scope, so its copy follows that profile's ``display.language`` and ``-p`` selector."""
    from gateway.run import _async_profile_runtime_scope
    from gateway.warning_notifications import present_notification
    from hermes_constants import get_routing_process_hermes_home

    served_homes = getattr(runner, "_served_profile_homes", None) or {}
    failure_fmt = f"{getattr(platform, 'value', platform)} attention notice failed for %s:%s: %s"
    sent: list[bool] = []
    for profile, target, _cfg, home, transport in list(runner._served_home_channel_transports()):
        if target == platform:
            continue
        served_home = served_homes.get(profile) if profile is not None else None
        if Path(served_home or get_routing_process_hermes_home()) != Path(profile_home):
            continue
        scope = _async_profile_runtime_scope(Path(served_home)) if served_home else contextlib.nullcontext()
        async with scope:
            message = render_message()

            async def send() -> None:
                sent.append(await runner._send_home_channel_message(target, home, transport, message, failure_fmt))

            await present_notification(send, platform=target)
    return sum(sent)
