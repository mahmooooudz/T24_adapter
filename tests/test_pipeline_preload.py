"""
Tests for T24GenericPipeline.preload() — the seam the worker pool uses to
skip already-done pre-work in each worker.

Network-free: we observe behaviour by inspecting state set on the pipeline
(no DB or files required).
"""

from t24_adapter.config import T24PipelineConfig
from t24_adapter.pipeline import T24GenericPipeline


def _cfg() -> T24PipelineConfig:
    # Smallest config that constructs a database-mode pipeline successfully.
    return T24PipelineConfig(
        package_root="t24_input_package",
        source="database",
        db_schema="t24_adaptor",
    )


def test_default_state_skips_nothing():
    p = T24GenericPipeline(_cfg())
    assert p._skip_validation is False
    assert p._preset_applications is None
    assert p._registry_cache == {}


def test_preload_sets_skip_validation_and_apps():
    p = T24GenericPipeline(_cfg())
    p.preload(applications=["ACCOUNT", "CUSTOMER"], skip_validation=True)
    assert p._skip_validation is True
    assert p._preset_applications == ["ACCOUNT", "CUSTOMER"]


def test_preload_populates_registry_cache_uppercased():
    """The cache is keyed by upper-cased app name (see Stage 3 lookup)."""
    p = T24GenericPipeline(_cfg())
    fake_a, fake_b = object(), object()
    p.preload(registries={"account": fake_a, "Customer": fake_b})
    assert p._registry_cache == {"ACCOUNT": fake_a, "CUSTOMER": fake_b}


def test_preload_is_additive_across_calls():
    p = T24GenericPipeline(_cfg())
    p.preload(skip_validation=True)
    p.preload(applications=["A"])
    p.preload(registries={"A": "reg-a"})
    assert p._skip_validation is True
    assert p._preset_applications == ["A"]
    assert p._registry_cache == {"A": "reg-a"}


def test_preload_with_no_args_is_a_noop():
    p = T24GenericPipeline(_cfg())
    p.preload()
    assert p._skip_validation is False
    assert p._preset_applications is None
    assert p._registry_cache == {}
