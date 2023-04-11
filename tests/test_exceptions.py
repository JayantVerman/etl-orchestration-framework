"""Tests for the exception hierarchy."""

from __future__ import annotations

import pytest

from etl_orchestrator.exceptions import (
    DependencyCycleError,
    DuplicateTaskError,
    OrchestrationError,
    SelfDependencyError,
    UnknownTaskError,
    WorkflowValidationError,
)


class TestExceptionHierarchy:
    def test_validation_errors_derive_from_base(self) -> None:
        for exc_type in (
            WorkflowValidationError,
            DuplicateTaskError,
            UnknownTaskError,
            SelfDependencyError,
            DependencyCycleError,
        ):
            assert issubclass(exc_type, OrchestrationError)
            assert issubclass(exc_type, Exception)

    def test_base_error_is_catchable_family(self) -> None:
        with pytest.raises(OrchestrationError):
            raise DuplicateTaskError("a")

    def test_workflow_validation_collects_errors(self) -> None:
        error = WorkflowValidationError(["bad thing 1", "bad thing 2"])
        assert error.errors == ["bad thing 1", "bad thing 2"]
        assert "bad thing 1" in str(error)
        assert "bad thing 2" in str(error)

    def test_duplicate_task_message(self) -> None:
        error = DuplicateTaskError("extract")
        assert error.task_id == "extract"
        assert "extract" in str(error)

    def test_unknown_task_message(self) -> None:
        error = UnknownTaskError("ghost")
        assert error.task_id == "ghost"
        assert "ghost" in str(error)

    def test_cycle_error_carries_cycle_path(self) -> None:
        error = DependencyCycleError(["a", "b", "c"])
        assert error.cycle == ["a", "b", "c"]
        assert "a -> b -> c" in str(error)
