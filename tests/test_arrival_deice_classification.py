from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from arrival_deice_utils import classify_deice_record
from deice_info_helper import DeiceRecord


def test_type_one_and_four_line_counts_as_full_deice():
    record = DeiceRecord(
        icao="KTEB",
        iata="TEB",
        faa="TEB",
        name="Teterboro",
        has_deice=True,
        deice_info="Type 1 and 4 Available",
        raw_deice_flag="No",
    )
    code, label = classify_deice_record(record)
    assert code == "full"
    assert "Types I & IV" in label
