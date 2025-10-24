import sys
import types
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest


class _ContextStub:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _SidebarStub:
    def header(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def success(self, *args, **kwargs):
        return None

    def toggle(self, label, value=False, **kwargs):
        return value

    def number_input(self, label, min_value=None, max_value=None, value=0, step=1, **kwargs):
        return value

    def text_input(self, label, value="", **kwargs):
        return value

    def slider(self, label, min_value=None, max_value=None, value=None, **kwargs):
        return value

    def checkbox(self, label, value=False, **kwargs):
        return value

    def caption(self, *args, **kwargs):
        return None

    def date_input(self, label, value=None, **kwargs):
        return value

    def metric(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None


class _StreamlitStub(types.ModuleType):
    def __init__(self):
        super().__init__("streamlit")
        self.secrets = {}
        self.session_state = {}
        self.sidebar = _SidebarStub()

    def cache_data(self, **kwargs):
        def decorator(func):
            return func

        return decorator

    def set_page_config(self, *args, **kwargs):
        return None

    def title(self, *args, **kwargs):
        return None

    def caption(self, *args, **kwargs):
        return None

    def subheader(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def success(self, *args, **kwargs):
        return None

    def error(self, *args, **kwargs):
        return None

    def tabs(self, labels):
        return [_ContextStub() for _ in labels]

    def columns(self, spec):
        if isinstance(spec, int):
            count = spec
        else:
            count = len(list(spec))
        return [_ContextStub() for _ in range(count)]

    def button(self, *args, **kwargs):
        return False

    def write(self, *args, **kwargs):
        return None

    def dataframe(self, *args, **kwargs):
        return None

    def metric(self, *args, **kwargs):
        return None

    def download_button(self, *args, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def stop(self):
        raise RuntimeError("st.stop() called")


def _ensure_stubs():
    if "streamlit" not in sys.modules:
        sys.modules["streamlit"] = _StreamlitStub()
    if "Home" not in sys.modules:
        home = types.ModuleType("Home")
        home.password_gate = lambda: None
        sys.modules["Home"] = home


@pytest.fixture(scope="module")
def task_splitter_module():
    _ensure_stubs()
    sys.path.append("AirSprint-Tools")
    module = __import__("pages.Task_Splitter", fromlist=["*"])
    return module


def _make_tail(tail_cls, tail: str, tz: str):
    dt = datetime(2024, 1, 1, 8, tzinfo=ZoneInfo(tz))
    return tail_cls(
        tail=tail,
        legs=1,
        workload=2.0,
        first_local_dt=dt,
        sample_legs=[],
    )


@pytest.fixture
def TailPackage(task_splitter_module):
    return task_splitter_module.TailPackage


@pytest.fixture
def assign_preference_weighted(task_splitter_module):
    return task_splitter_module.assign_preference_weighted


@pytest.fixture
def is_easterly_offset(task_splitter_module):
    return task_splitter_module._is_easterly_offset


@pytest.fixture
def is_westerly_offset(task_splitter_module):
    return task_splitter_module._is_westerly_offset


def test_force_easterly_option_moves_work_when_needed(TailPackage, assign_preference_weighted):
    labels = ["0500", "0600", "0800", "0900"]
    weights = [1.0] * len(labels)
    packages = [
        _make_tail(TailPackage, f"E{i}", "America/New_York") for i in range(6)
    ] + [
        _make_tail(TailPackage, "C1", "America/Chicago")
    ]

    buckets = assign_preference_weighted(
        packages,
        labels,
        weights,
        force_easterly_first=True,
    )

    east_tails = {pkg.tail for pkg in packages if pkg.tail.startswith("E")}
    other_buckets = labels[1:]
    moved = any(
        pkg.tail in east_tails
        for label in other_buckets
        for pkg in buckets.get(label, [])
    )
    assert moved, "Expected at least one easterly tail to move west when balancing"


def test_is_easterly_offset_bounds(is_easterly_offset):
    assert is_easterly_offset(-5.0)
    assert is_easterly_offset(-3.5)
    assert not is_easterly_offset(-6.0)
    assert not is_easterly_offset(-1.5)
    assert not is_easterly_offset(0.0)


def test_westerly_tails_prefer_last_shift(TailPackage, assign_preference_weighted):
    labels = ["0500", "0800", "1200"]
    weights = [1.0, 1.0, 2.0]
    packages = [
        _make_tail(TailPackage, "W1", "America/Los_Angeles"),
        _make_tail(TailPackage, "W2", "America/Denver"),
        _make_tail(TailPackage, "C1", "America/Chicago"),
        _make_tail(TailPackage, "E1", "America/New_York"),
    ]

    buckets = assign_preference_weighted(packages, labels, weights)

    western_tails = {"W1", "W2"}
    last_shift = labels[-1]
    last_shift_tails = {pkg.tail for pkg in buckets.get(last_shift, [])}
    assert western_tails.issubset(last_shift_tails)


def test_is_westerly_offset_bounds(is_westerly_offset):
    assert is_westerly_offset(-8.0)
    assert is_westerly_offset(-7.0)
    assert not is_westerly_offset(-6.0)
    assert not is_westerly_offset(-4.0)
