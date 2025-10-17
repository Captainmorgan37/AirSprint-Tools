"""Utilities for loading the Morning Report automation plan."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Dict, Iterable, List, Sequence


@dataclass(frozen=True)
class ReportPlan:
    """Structured information about a single report entry in the plan."""

    code: str
    title: str
    current_capability: str
    gaps: str
    next_steps: str
    sample_outputs: List[str]

    def short_name(self) -> str:
        return f"{self.code} {self.title}".strip()


_SECTION_PATTERN = re.compile(
    r"^## (?P<header>[^\n]+)\n(?P<body>.*?)(?=\n## |\Z)",
    re.DOTALL | re.MULTILINE,
)


def _extract_field(section_body: str, label: str) -> str:
    pattern = re.compile(
        rf"- \*\*{re.escape(label)}:\*\* (?P<value>.*?)(?=\n- \*\*|\Z)",
        re.DOTALL,
    )
    match = pattern.search(section_body)
    if not match:
        return ""
    value = match.group("value").strip()
    return value


def _extract_code_blocks(section_body: str) -> List[str]:
    return [block.strip() for block in re.findall(r"```(.*?)```", section_body, re.DOTALL)]


def _parse_header(header: str) -> Dict[str, str]:
    parts = header.strip().split(" ", 1)
    if len(parts) == 1:
        return {"code": parts[0], "title": ""}
    return {"code": parts[0], "title": parts[1]}


def load_plan(path: Path | str, *, include: Iterable[str] | None = None) -> Dict[str, ReportPlan]:
    """Load report definitions from the markdown plan.

    Parameters
    ----------
    path:
        File path to ``FL3XX-report-automation-plan.md``.
    include:
        Optional iterable of report codes to filter by. When omitted all
        sections are parsed.
    """

    markdown = Path(path).read_text(encoding="utf-8")
    include_set = {code.strip() for code in include} if include else None

    reports: Dict[str, ReportPlan] = {}

    for match in _SECTION_PATTERN.finditer(markdown):
        header = match.group("header")
        body = match.group("body")
        header_data = _parse_header(header)
        code = header_data.get("code", "").strip()
        if include_set and code not in include_set:
            continue

        current = _extract_field(body, "Current Capability")
        gaps = _extract_field(body, "Gaps / Required Inputs")
        next_steps = _extract_field(body, "Next Steps")
        code_blocks = _extract_code_blocks(body)

        reports[code] = ReportPlan(
            code=code,
            title=header_data.get("title", ""),
            current_capability=current,
            gaps=gaps,
            next_steps=next_steps,
            sample_outputs=code_blocks,
        )

    if include_set:
        missing = sorted(include_set.difference(reports.keys()))
        if missing:
            raise ValueError(
                "Plan file is missing expected report codes: " + ", ".join(missing)
            )

    return reports


def list_reports(path: Path | str, *, include: Sequence[str] | None = None) -> List[ReportPlan]:
    """Return plan entries as a list, sorted by report code."""

    reports = load_plan(path, include=include)
    return [reports[key] for key in sorted(reports.keys())]
