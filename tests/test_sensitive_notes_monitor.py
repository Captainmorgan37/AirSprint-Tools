import importlib.util
import sys
from pathlib import Path


def _load_dashboard_module():
    root = Path(__file__).resolve().parents[1]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    module_name = "owner_services_dashboard_for_tests"
    spec = importlib.util.spec_from_file_location(
        module_name, root / "pages" / "Owner Services Dashboard.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_highlight_keywords_marks_sensitive_terms():
    module = _load_dashboard_module()

    note_text = "1 x Perrier can or bottle per guest at room temperature"
    highlighted, matches = module._highlight_keywords(note_text)

    assert matches == ["PERRIER"]
    assert "<mark>Perrier</mark>" in highlighted
