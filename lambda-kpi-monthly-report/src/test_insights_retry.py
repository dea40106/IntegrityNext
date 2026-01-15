import pytest
from botocore.exceptions import ClientError

from src.insights import call_with_retries


def _throttling_error():
    return ClientError(
        error_response={
            "Error": {"Code": "TooManyRequestsException", "Message": "throttle"},
            "ResponseMetadata": {"HTTPStatusCode": 429},
        },
        operation_name="StartQuery",
    )


def test_call_with_retries_eventually_succeeds(monkeypatch):
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _throttling_error()
        return "ok"

    # make sleep no-op for test speed
    monkeypatch.setattr("time.sleep", lambda *_: None)

    assert call_with_retries(fn, max_retries=5, what="test") == "ok"
    assert calls["n"] == 3


def test_call_with_retries_gives_up(monkeypatch):
    def fn():
        raise _throttling_error()

    monkeypatch.setattr("time.sleep", lambda *_: None)

    with pytest.raises(ClientError):
        call_with_retries(fn, max_retries=2, what="test")
