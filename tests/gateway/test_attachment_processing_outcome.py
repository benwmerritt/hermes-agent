"""Media-only replies use delivery receipts when reporting the turn outcome."""
import asyncio
from unittest.mock import AsyncMock

import pytest

from gateway.platforms.base import SendResult
from gateway.platforms.event import ProcessingOutcome
from gateway.session import build_session_key
from tests.gateway.test_base_topic_sessions import DummyTelegramAdapter, _make_event
from tests.gateway.test_73771_media_resend_dedup import _allowed_file


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['document', 'image'])
@pytest.mark.parametrize('outcome', ['success', 'failure', 'exception'])
async def test_media_only_turn_records_actual_delivery(tmp_path, monkeypatch, kind, outcome):
    adapter = DummyTelegramAdapter()
    adapter.config.typing_indicator = False
    sender = AsyncMock(return_value=SendResult(success=outcome == 'success', message_id='media-1'))
    if outcome == 'exception':
        sender.side_effect = RuntimeError('injected media transport failure')
    if kind == 'document':
        path = _allowed_file(tmp_path, monkeypatch, 'canary.txt')
        response = f'MEDIA:{path}'
        adapter.send_document = sender
    else:
        response = '![canary](https://example.test/canary.png)'
        adapter.send_image = sender
    adapter.set_message_handler(lambda event: asyncio.sleep(0, result=response))
    event = _make_event('chat', 'topic')
    await adapter._process_message_background(event, build_session_key(event.source))
    sender.assert_awaited_once()
    expected = ProcessingOutcome.SUCCESS if outcome == 'success' else ProcessingOutcome.FAILURE
    assert adapter.processing_hooks[-1] == ('complete', '1', expected)


@pytest.mark.asyncio
async def test_legacy_image_batch_without_receipts_does_not_imply_success():
    adapter = DummyTelegramAdapter()
    adapter.config.typing_indicator = False
    adapter.send_multiple_images = AsyncMock(return_value=None)
    adapter.set_message_handler(lambda event: asyncio.sleep(0, result='![canary](https://example.test/canary.png)'))
    event = _make_event('chat', 'topic')
    await adapter._process_message_background(event, build_session_key(event.source))
    adapter.send_multiple_images.assert_awaited_once()
    assert adapter.processing_hooks[-1] == ('complete', '1', ProcessingOutcome.FAILURE)
