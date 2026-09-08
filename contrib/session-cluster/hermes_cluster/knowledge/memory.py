"""Native memory validation and bounded prompt projection over per-entry CAS."""
import difflib
from tools.memory_tool_store import MemoryStore, ENTRY_DELIMITER
from .store import digest


class SharedMemoryStore(MemoryStore):
    def __init__(self, runtime, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.runtime = runtime

    def _rows(self, target, resources=None):
        resources = self.runtime.resources.values() if resources is None else resources
        return [r for r in resources if r["kind"] == "memory" and r.get("value") is not None
                and r["value"]["target"] == target]

    def load_from_disk(self):
        # Sanitize and bound the immutable prompt projection using the native loader.
        from hermes_constants import set_hermes_home_override, reset_hermes_home_override
        token = set_hermes_home_override(self.runtime.snapshot_home)
        try:
            super().load_from_disk()
        finally:
            reset_hermes_home_override(token)
        for target in ("memory", "user"):
            entries = self._entries_for(target)
            bounded = []
            for entry in entries:
                if len(ENTRY_DELIMITER.join(bounded + [entry])) <= self._char_limit(target):
                    bounded.append(entry)
            # _render_block cannot introduce unsanitized content: keep only native sanitized entries.
            raw = self._system_prompt_snapshot[target]
            if len(ENTRY_DELIMITER.join(entries)) > self._char_limit(target):
                from tools.memory_tool_store import _scan_memory_content
                safe = [e for e in bounded if not _scan_memory_content(e)]
                self._system_prompt_snapshot[target] = self._render_block(target, safe)
            else:
                self._system_prompt_snapshot[target] = raw
            self._set_entries(target, [r["value"]["content"] for r in self._rows(target)])

    def _mutate(self, target, mutate, *, skip_drift=False):
        with self.runtime.lock:
            rows = self._rows(target)
            before = [r["value"]["content"] for r in rows]
            self._set_entries(target, before)
            result = mutate(before[:], self._char_limit(target))
            if isinstance(result, dict):
                return result
            after, message = result
            operations = []
            for tag, i, j, a, b in difflib.SequenceMatcher(a=before, b=after, autojunk=False).get_opcodes():
                if tag == "equal":
                    continue
                old, new = rows[i:j], after[a:b]
                for index in range(max(len(old), len(new))):
                    row = old[index] if index < len(old) else None
                    content = new[index] if index < len(new) else None
                    if row and row["audience"] != self.runtime.snapshot["audience"]:
                        return {"success": False, "error": "read-only audience entry cannot be modified"}
                    key = row["key"] if row else digest([target, content])
                    known = self.runtime.resources.get(("memory", key, self.runtime.snapshot["audience"]))
                    operations.append({"key": key, "expected_revision": row["revision"] if row else (
                        known["revision"] if known else 0),
                        "value": {"target": target, "content": content} if content is not None else None})
            if not operations:
                return self._success_response(target, message)
            try:
                receipt = self.runtime.client.mutate("/v1/memory/mutations", {"operations": operations})
                self.runtime.accept(receipt)
            except Exception as exc:
                return {"success": False, "error": f"Shared memory write failed: {exc}"}
            self._set_entries(target, [r["value"]["content"] for r in self._rows(target)])
            return self._success_response(target, message) | {"receipt": receipt["mutation_id"]}
