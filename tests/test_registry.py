"""Unit tests for the plugin-based job registry."""

import pytest

from app.registry.job_registry import (
    get_handler,
    is_registered,
    job_handler,
    list_registered,
)


def test_register_and_retrieve():
    """A handler registered with @job_handler should be retrievable by type."""

    @job_handler("test_job_type_unique")
    def my_handler(payload):
        return {"done": True}

    handler = get_handler("test_job_type_unique")
    assert handler({"x": 1}) == {"done": True}


def test_unknown_type_raises():
    """Looking up an unregistered job_type should raise KeyError."""
    with pytest.raises(KeyError):
        get_handler("nonexistent_type_xyz")


def test_unknown_type_error_lists_available_handlers():
    """The error must be actionable — the cause is almost always a typo."""
    import handlers.builtin  # noqa: F401 — populates the registry

    with pytest.raises(KeyError) as exc_info:
        get_handler("send_emial")
    assert "send_email" in str(exc_info.value)


def test_duplicate_registration_is_rejected():
    """Silently overwriting a handler would disable a job type invisibly."""

    @job_handler("test_duplicate_guard")
    def first(payload):
        return {"from": "first"}

    with pytest.raises(ValueError, match="already handled"):

        @job_handler("test_duplicate_guard")
        def second(payload):
            return {"from": "second"}

    assert get_handler("test_duplicate_guard")({}) == {"from": "first"}


def test_builtin_handlers_are_registered_on_import():
    """Importing the handlers module is what populates the registry.

    This is the mechanism that lets the worker stay ignorant of what job types
    exist — it imports one module and looks everything up by name.
    """
    import handlers.builtin  # noqa: F401

    registered = list_registered()
    assert {"send_email", "resize_image", "generate_report"} <= set(registered)


def test_is_registered():
    import handlers.builtin  # noqa: F401

    assert is_registered("send_email") is True
    assert is_registered("definitely_not_a_job") is False


def test_builtin_handler_returns_a_serialisable_result():
    """A handler's return value becomes the job's JSONB result column.

    ``resize_image`` now uses Pillow to perform a real in-memory resize.
    The result contains ``output_width`` / ``output_height`` (the actual
    dimensions of the resized image) rather than echoing the input payload.
    """
    import json

    import handlers.builtin as builtin

    result = builtin.resize_image({"width": 100, "height": 50, "url": "a.png"})
    # The handler resizes to the requested dimensions.
    assert result["output_width"] == 100
    assert result["output_height"] == 50
    assert result["format"] == "JPEG"
    assert result["output_bytes"] > 0
    json.dumps(result)  # must not raise — result must be JSON-serialisable

