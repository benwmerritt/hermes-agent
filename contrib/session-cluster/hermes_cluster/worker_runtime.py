"""Native gateway ownership policy for one retained Discord conversation.

Import only after preparing HERMES_HOME and its immutable knowledge snapshot.
"""
from __future__ import annotations

from gateway.config import Platform, PlatformConfig
from gateway.relay.adapter import RelayAdapter
from gateway.run import GatewayRunner
from gateway.session import SessionSource, build_session_key


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

    async def _start_post_connect_services(self, connected_count: int) -> None:
        # Retain process liveness and plugin startup, without starting hosted-room
        # workers or user-created heartbeat schedules in every conversation Pod.
        self._start_loop_heartbeat_task()
        await self.hooks.emit("gateway:startup", {"platforms": [p.value for p in self.adapters]})

    def _is_user_authorized(self, source, *, allow_adapter_delegation=True) -> bool:
        return self.policy.accepts(source)

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
