"""Native gateway ownership policy for one retained Discord conversation.

Import only after preparing HERMES_HOME and its immutable knowledge snapshot.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import logging

from gateway.config import Platform, PlatformConfig
from gateway.relay.adapter import RelayAdapter
from gateway.run import GatewayRunner, _ADAPTER_DISCONNECT_TIMEOUT_SECS_DEFAULT
from gateway.session import SessionSource, build_session_key

logger = logging.getLogger(__name__)


class ConversationPolicy:
    def __init__(self, config: dict):
        self.config = config
        self.source = SessionSource.from_dict(config["source"])
        self.key = config["conversation_key"]
        self.allowed_users = frozenset(config["allowed_user_ids"])
        if build_session_key(self.source) != self.key:
            raise ValueError("configured conversation key does not match its native source")

    def accepts(self, source: SessionSource, *, require_actor: bool = True) -> bool:
        if source is None or source.platform != Platform.DISCORD:
            return False
        fields = ("chat_id", "chat_type", "thread_id", "scope_id", "parent_chat_id", "profile")
        if any((getattr(source, field, None) or None) != (getattr(self.source, field, None) or None) for field in fields):
            return False
        if build_session_key(source) != self.key:
            return False
        return not require_actor or source.user_id in self.allowed_users


class ConversationRelayAdapter(RelayAdapter):
    def __init__(self, config, descriptor, transport, policy: ConversationPolicy):
        super().__init__(config, descriptor, transport)
        self.policy = policy

    async def _on_inbound(self, event) -> None:
        # Approval responses are consumed before the runner's authorization gate.
        if not self.policy.accepts(event.source):
            raise PermissionError("relay event does not belong to this conversation owner")
        if event.get_command() == "cluster-stop-worker":
            # Lifecycle control must bypass both native busy-session queues.
            await self.gateway_runner._handle_message(event)
            return
        await super()._on_inbound(event)

    async def on_interrupt(self, session_key: str, chat_id: str) -> None:
        if session_key != self.policy.key or chat_id != self.policy.source.chat_id:
            raise PermissionError("interrupt does not belong to this conversation")
        await super().on_interrupt(session_key, chat_id)

    async def _on_passthrough(self, forward, buffer_id=None) -> None:
        raise PermissionError("worker accepts normalized authorized events only")


class ConversationGateway(GatewayRunner):
    # Process completion and delegate delivery are session-owned. Shared cron,
    # kanban, curator and auth-keepalive are deliberately not started here.
    _PRE_RECONNECT_WATCHERS = (
        "_session_housekeeping_watcher", "_session_stall_watcher",
    )
    _POST_RECONNECT_WATCHERS = ("_async_delegation_watcher",)
    _CONTROLLER_COMMANDS = frozenset({"restart", "update", "gateway", "cron", "schedule", "loop", "goal", "kanban", "heartbeat"})

    def __init__(self, policy: ConversationPolicy):
        self.policy = policy
        super().__init__()
        self.config.multiplex_profiles = False
        self.config.group_sessions_per_user = True
        self.config.thread_sessions_per_user = False
        self.config.platforms = {Platform.RELAY: PlatformConfig(
            enabled=True, extra={"relay_url": policy.config["relay"]["url"]})}

    def _instantiate_adapter(self, platform, config):
        if platform != Platform.RELAY:
            raise ValueError("conversation workers may only connect the Relay adapter")
        adapter = super()._instantiate_adapter(platform, config)
        if adapter is None:
            return None
        return ConversationRelayAdapter(config, adapter.descriptor, adapter._transport, self.policy)

    def _schedule_resume_pending_sessions(self, platform=None) -> int:
        # A new process cannot establish whether an interrupted external effect
        # committed. Preserve native pending state for an explicit human follow-up.
        return 0

    async def _bounded_adapter_teardown(self, adapter, platform, *, profile=None) -> None:
        # Native agent drain ends before adapter-owned final sends and their ledger
        # acknowledgements. Let those settle before native teardown cancels them.
        if platform == Platform.RELAY:
            timeout = self._adapter_disconnect_timeout_secs()
            if timeout <= 0:
                timeout = _ADAPTER_DISCONNECT_TIMEOUT_SECS_DEFAULT
            deadline = asyncio.get_running_loop().time() + timeout
            current = asyncio.current_task()
            while pending := {task for task in adapter._session_tasks.values()
                              if task is not current and not task.done()}:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    self._exit_code = 1
                    self._exit_reason = "Relay delivery cleanup timed out; reconcile retained obligations"
                    logger.error(self._exit_reason)
                    break
                await asyncio.wait(pending, timeout=remaining)
        await super()._bounded_adapter_teardown(adapter, platform, profile=profile)

    async def _claim_pending_obligations(self) -> list:
        # A prior Discord send may have committed before its acknowledgement.
        # Keep the native obligation and its full content untouched for operator
        # reconciliation; a new Relay request ID cannot deduplicate that send.
        return []

    async def _redeliver_failed_obligations_for_platform(self, platform, *, profile=None) -> int:
        return 0

    async def _arm_flood_timers_for_waiting_rows(self) -> None:
        return None

    def _schedule_flood_redelivery(self, platform, *, profile=None) -> None:
        return None

    async def _start_post_connect_services(self, connected_count: int) -> None:
        # Retain process liveness and plugin startup, without starting hosted-room
        # workers or user-created heartbeat schedules in every conversation Pod.
        self._start_loop_heartbeat_task()
        await self.hooks.emit("gateway:startup", {"platforms": [p.value for p in self.adapters]})

    def _is_user_authorized(self, source, *, allow_adapter_delegation=True) -> bool:
        return self.policy.accepts(source)

    async def _hmwa_first_contact_notes(self, source, history, turn_sidecar_notes):
        # Reuse native onboarding, whose LOCAL branch omits only home-channel
        # setup. This helper-only copy never changes the event or delivery route;
        # a conversation worker has no shared cron/cross-platform home to set up.
        await super()._hmwa_first_contact_notes(
            replace(source, platform=Platform.LOCAL), history, turn_sidecar_notes)

    async def _interrupt_and_clear_session(self, session_key, source, **kwargs) -> None:
        await super()._interrupt_and_clear_session(session_key, source, **kwargs)
        # The native hard stop can retire the agent/executor before an approval
        # wait observes its thread-local interrupt bit. Wake that wait explicitly
        # after invalidating the run, so it cannot execute and keep the Pod alive.
        from tools.approval import resolve_gateway_approval
        resolve_gateway_approval(session_key, "deny", resolve_all=True, reason="Conversation interrupted")

    async def _handle_message(self, event):
        if not self.policy.accepts(event.source, require_actor=not event.internal):
            return "This worker cannot access that conversation."
        if event.get_command() == "cluster-stop-worker" and not event.internal:
            self._draining = True
            adapter = self._adapter_for_source(event.source)
            if adapter is not None:
                await adapter.send(event.source.chat_id, "Worker shutdown accepted. Waiting for native cleanup and history publication.",
                                   metadata={"thread_id": event.source.thread_id} if event.source.thread_id else None)
            self._request_clean_exit("cluster controller requested worker shutdown")
            return None
        if event.get_command() in self._CONTROLLER_COMMANDS:
            return "This conversation worker does not run shared schedulers or manage its lifecycle. Use the cluster controller."
        return await super()._handle_message(event)

    def request_restart(self, *, detached=False, via_service=False) -> bool:
        return False
