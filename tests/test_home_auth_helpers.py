from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import MappingProxyType


def _load_home_module():
    home_path = Path(__file__).resolve().parents[1] / "Home.py"
    spec = spec_from_file_location("home_module", home_path)
    assert spec and spec.loader
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_to_plain_data_converts_immutable_mappings_and_sequences() -> None:
    home_module = _load_home_module()

    source = MappingProxyType(
        {
            "usernames": MappingProxyType(
                {
                    "ops_admin": MappingProxyType(
                        {"name": "Ops Admin", "role": "admin", "tags": ("a", "b")}
                    )
                }
            )
        }
    )

    converted = home_module._to_plain_data(source)

    assert isinstance(converted, dict)
    assert isinstance(converted["usernames"], dict)
    assert isinstance(converted["usernames"]["ops_admin"], dict)
    assert converted["usernames"]["ops_admin"]["tags"] == ["a", "b"]

    converted["usernames"]["ops_admin"]["new_key"] = "ok"
    assert converted["usernames"]["ops_admin"]["new_key"] == "ok"
