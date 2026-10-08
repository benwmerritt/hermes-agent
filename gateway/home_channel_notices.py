"""Operator notices to the home channels of the profile that owns a problem.

One host process serves several profiles, so a notice about one profile's resource (its cron store,
its WhatsApp session) must reach that profile's home channels only, rendered in its language and
behind its ``display.suppress_warning_notifications`` opt-out. Callers: ``cron_store_notices`` and
the WhatsApp adapter's logged-out notice.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Callable


async def send_profile_home_notice(
    runner, owns_home: Callable[[Path], bool], render_message: Callable[[], str], failure_fmt: str, *,
    skip_platform=None,
) -> int:
    """Post ``render_message()`` to every live home channel of each served profile whose home
    ``owns_home`` accepts, except ``skip_platform``'s; returns how many channels accepted it.
    The message is rendered inside the owning profile's scope, so its copy follows that profile's
    ``display.language`` and ``-p`` selector."""
    from gateway.run import _async_profile_runtime_scope
    from gateway.warning_notifications import present_notification
    from hermes_constants import get_routing_process_hermes_home

    served_homes = runner._served_profile_homes or {}
    delivered = 0
    for profile, platform, _cfg, home, transport in list(runner._served_home_channel_transports()):
        if platform == skip_platform:
            continue
        served_home = served_homes.get(profile) if profile is not None else None
        if not owns_home(Path(served_home or get_routing_process_hermes_home())):
            continue
        # The opt-out is the owning profile's; the launch profile needs no extra scope.
        scope = _async_profile_runtime_scope(Path(served_home)) if served_home else contextlib.nullcontext()
        async with scope:
            message = render_message()
            accepted = False

            async def send() -> None:
                nonlocal accepted
                accepted = await runner._send_home_channel_message(platform, home, transport, message, failure_fmt)

            await present_notification(send, platform=platform)
            delivered += accepted
    return delivered
