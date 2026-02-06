from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import streamlit as st

from Home import configure_page, password_gate, render_sidebar


FEE_RATE = 0.10
DEFAULT_CATERING_ROWS = 6
DEFAULT_ADDITIONAL_ROWS = 3
PROVINCE_TAX_RATES: Dict[str, float] = {
    "AB": 5.0,
    "NT": 5.0,
    "NU": 5.0,
    "YT": 5.0,
    "SK": 11.0,
    "BC": 12.0,
    "MB": 12.0,
    "ON": 13.0,
    "QC": 14.975,
    "NS": 15.0,
    "NB": 15.0,
    "NL": 15.0,
    "PE": 15.0,
}
PROVINCE_TAX_LABELS: Dict[str, str] = {
    "AB": "GST 5%",
    "NT": "GST 5%",
    "NU": "GST 5%",
    "YT": "GST 5%",
    "SK": "GST 5% + PST 6% = 11%",
    "BC": "GST 5% + PST 7% = 12%",
    "MB": "GST 5% + PST 7% = 12%",
    "ON": "HST 13%",
    "QC": "GST + QST 14.975%",
    "NS": "HST 15%",
    "NB": "HST 15%",
    "NL": "HST 15%",
    "PE": "HST 15%",
}
PROVINCE_NAME_TO_CODE: Dict[str, str] = {
    "Alberta": "AB",
    "Northwest Territories": "NT",
    "Nunavut": "NU",
    "Yukon": "YT",
    "Saskatchewan": "SK",
    "British Columbia": "BC",
    "Manitoba": "MB",
    "Ontario": "ON",
    "Quebec": "QC",
    "Nova Scotia": "NS",
    "New Brunswick": "NB",
    "Newfoundland and Labrador": "NL",
    "Prince Edward Island": "PE",
}


@dataclass
class CateringBreakdown:
    items_total: float
    delivery_fees: float
    airport_fee: float
    taxes: float
    air_sprint_fee: float
    grand_total: float
    total_catering: float


@dataclass
class GTBreakdown:
    base: float
    gratuity: float
    additional_total: float
    credit_card_fee: float
    air_sprint_fee: float
    taxes: float
    grand_total: float
    additional_lines: List[Tuple[str, float]]


def _currency(value: float) -> str:
    return f"${value:,.2f}"


def _ensure_dataframe(df: pd.DataFrame, columns: Iterable[str], rows: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame({column: ["" for _ in range(rows)] for column in columns})
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df[columns]


def _default_catering_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Item": ["" for _ in range(DEFAULT_CATERING_ROWS)],
            "Unit Price": [0.0 for _ in range(DEFAULT_CATERING_ROWS)],
            "Qty": [0.0 for _ in range(DEFAULT_CATERING_ROWS)],
        }
    )


def _default_additional_rows() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Label": ["" for _ in range(DEFAULT_ADDITIONAL_ROWS)],
            "Type": ["%" for _ in range(DEFAULT_ADDITIONAL_ROWS)],
            "Value": [0.0 for _ in range(DEFAULT_ADDITIONAL_ROWS)],
        }
    )


def _coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _calculate_items_total(items: pd.DataFrame) -> float:
    unit_price = _coerce_numeric(items.get("Unit Price", pd.Series(dtype=float)))
    qty = _coerce_numeric(items.get("Qty", pd.Series(dtype=float)))
    return float((unit_price * qty).sum())


@st.cache_data
def _load_canadian_airport_provinces() -> Dict[str, str]:
    df = pd.read_csv("Airport TZ.txt")
    df = df[df["country"] == "CA"]
    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        province_name = str(row.get("subd", "")).strip()
        province_code = PROVINCE_NAME_TO_CODE.get(province_name)
        if not province_code:
            continue
        for key in ("icao", "iata"):
            value = str(row.get(key, "")).strip().upper()
            if value and value != "NAN":
                mapping[value] = province_code
    return mapping


def _lookup_province_code(airport_code: str) -> str | None:
    airport_code = airport_code.strip().upper()
    if not airport_code:
        return None
    return _load_canadian_airport_provinces().get(airport_code)


def _tax_rate_from_selection(selection: str) -> float:
    for code, label in PROVINCE_TAX_LABELS.items():
        if selection.startswith(code):
            return PROVINCE_TAX_RATES[code]
    return 0.0


def _tax_rate_options() -> List[str]:
    return [f"{code} - {PROVINCE_TAX_LABELS[code]}" for code in PROVINCE_TAX_LABELS]


def compute_catering_canada(
    items: pd.DataFrame,
    delivery_fees: float,
    tax_rate: float,
    airport_fee_rate: float,
) -> CateringBreakdown:
    items_total = _calculate_items_total(items)
    total_catering = items_total
    air_sprint_fee = total_catering * FEE_RATE
    subtotal_with_fee = total_catering + air_sprint_fee
    taxes = total_catering * (tax_rate / 100)
    airport_fee = total_catering * (airport_fee_rate / 100)
    grand_total = delivery_fees + subtotal_with_fee + taxes + airport_fee
    return CateringBreakdown(
        items_total=items_total,
        delivery_fees=delivery_fees,
        airport_fee=airport_fee,
        taxes=taxes,
        air_sprint_fee=air_sprint_fee,
        grand_total=grand_total,
        total_catering=total_catering,
    )


def compute_catering_us(
    items: pd.DataFrame,
    delivery_airport_fees: float,
    tax_rate: float,
    airport_fee_rate: float,
) -> CateringBreakdown:
    items_total = _calculate_items_total(items)
    total_catering = items_total + delivery_airport_fees
    airport_fee = total_catering * (airport_fee_rate / 100)
    taxes = (tax_rate / 100) * (total_catering + airport_fee)
    pre_fee_total = total_catering + airport_fee + taxes
    air_sprint_fee = pre_fee_total * FEE_RATE
    grand_total = pre_fee_total + air_sprint_fee
    return CateringBreakdown(
        items_total=items_total,
        delivery_fees=delivery_airport_fees,
        airport_fee=airport_fee,
        taxes=taxes,
        air_sprint_fee=air_sprint_fee,
        grand_total=grand_total,
        total_catering=total_catering,
    )


def _parse_additional_charges(rows: pd.DataFrame, base: float) -> Tuple[float, List[Tuple[str, float]]]:
    total = 0.0
    lines: List[Tuple[str, float]] = []
    for _, row in rows.iterrows():
        label = str(row.get("Label", "")).strip() or "Additional"
        charge_type = str(row.get("Type", "%")).strip()
        value = row.get("Value", 0.0)
        try:
            value_float = float(value)
        except (TypeError, ValueError):
            value_float = 0.0

        if charge_type == "%":
            amount = base * (value_float / 100)
        else:
            amount = value_float

        if amount != 0.0:
            lines.append((label, amount))
        total += amount
    return total, lines


def compute_gt_canada(
    base: float,
    cc_rate: float,
    tax_rate: float,
    gratuity_rate: float,
    fuel_rate: float,
) -> GTBreakdown:
    taxes = base * (tax_rate / 100)
    gratuity = base * (gratuity_rate / 100)
    fuel = base * (fuel_rate / 100)
    credit_card_fee = base * (cc_rate / 100)
    pre_fee_total = base + fuel + credit_card_fee
    air_sprint_fee = pre_fee_total * FEE_RATE
    total_with_fee = pre_fee_total + air_sprint_fee
    grand_total = total_with_fee + taxes + gratuity
    return GTBreakdown(
        base=base,
        gratuity=gratuity,
        additional_total=fuel,
        credit_card_fee=credit_card_fee,
        air_sprint_fee=air_sprint_fee,
        taxes=taxes,
        grand_total=grand_total,
        additional_lines=[("Fuel surcharge", fuel)] if fuel else [],
    )


def compute_gt_us(
    base: float,
    cc_rate: float,
    gratuity_rate: float,
    additional_rows: pd.DataFrame,
) -> GTBreakdown:
    gratuity = base * (gratuity_rate / 100)
    additional_total, additional_lines = _parse_additional_charges(additional_rows, base)
    credit_card_fee = (base + gratuity + additional_total) * (cc_rate / 100)
    pre_fee_total = base + gratuity + additional_total + credit_card_fee
    air_sprint_fee = pre_fee_total * FEE_RATE
    grand_total = pre_fee_total + air_sprint_fee
    return GTBreakdown(
        base=base,
        gratuity=gratuity,
        additional_total=additional_total,
        credit_card_fee=credit_card_fee,
        air_sprint_fee=air_sprint_fee,
        taxes=0.0,
        grand_total=grand_total,
        additional_lines=additional_lines,
    )


def _init_state() -> None:
    if "catering_items" not in st.session_state:
        st.session_state.catering_items = _default_catering_rows()
    if "gt_additional" not in st.session_state:
        st.session_state.gt_additional = _default_additional_rows()
    if "catering_result" not in st.session_state:
        st.session_state.catering_result = None
    if "gt_result" not in st.session_state:
        st.session_state.gt_result = None


def _reset_state() -> None:
    st.session_state.catering_items = _default_catering_rows()
    st.session_state.gt_additional = _default_additional_rows()
    st.session_state.catering_result = None
    st.session_state.gt_result = None
    for key in list(st.session_state.keys()):
        if key.startswith("catering_") or key.startswith("gt_"):
            if key not in {"catering_items", "gt_additional", "catering_result", "gt_result"}:
                st.session_state.pop(key, None)


configure_page(page_title="Catering/GT Calculator")
password_gate()
render_sidebar()

st.title("Catering + GT Calculator")

_init_state()

if st.button("Reset"):
    _reset_state()
    st.rerun()

catering_tab, gt_tab = st.tabs(["Catering", "GT"])

with catering_tab:
    st.subheader("Catering Calculator")

    if st.button("Add catering row"):
        st.session_state.catering_items = pd.concat(
            [st.session_state.catering_items, _default_catering_rows().iloc[:1]],
            ignore_index=True,
        )

    with st.form("catering_form"):
        region = st.selectbox("Region", ["Canada", "US/International"], key="catering_region")
        items_df = _ensure_dataframe(
            st.session_state.catering_items,
            ["Item", "Unit Price", "Qty"],
            DEFAULT_CATERING_ROWS,
        )
        items_df = st.data_editor(
            items_df,
            key="catering_items_editor",
            use_container_width=True,
            column_config={
                "Item": st.column_config.TextColumn("Item"),
                "Unit Price": st.column_config.NumberColumn("Unit Price", min_value=0.0, step=0.01),
                "Qty": st.column_config.NumberColumn("Qty", min_value=0.0, step=1.0),
            },
        )

        if region == "Canada":
            delivery_fees = st.number_input("Delivery Fees ($)", min_value=0.0, step=0.01, key="catering_delivery")
            airport_code = st.text_input("Airport (ICAO/IATA)", key="catering_airport_code")
            province_code = _lookup_province_code(airport_code)
            if airport_code and not province_code:
                st.warning("Province not found for that airport code. Please select the tax rate manually.")
            if province_code:
                st.caption(f"Province detected: {province_code}")
                tax_rate = st.number_input(
                    "Tax Rate (%)",
                    value=PROVINCE_TAX_RATES[province_code],
                    disabled=True,
                    key="catering_tax_auto",
                )
            else:
                tax_selection = st.selectbox(
                    "Tax Rate (%)",
                    options=_tax_rate_options(),
                    key="catering_tax_selection",
                )
                tax_rate = _tax_rate_from_selection(tax_selection)
            airport_fee_rate = st.number_input(
                "Airport Fee Rate (%)", min_value=0.0, step=0.01, key="catering_airport_fee"
            )
        else:
            delivery_fees = st.number_input(
                "Delivery/Airport Fees ($)", min_value=0.0, step=0.01, key="catering_delivery_us"
            )
            tax_rate = st.number_input("Tax Rate (%)", min_value=0.0, step=0.01, key="catering_tax_us")
            airport_fee_rate = st.number_input(
                "Airport Fee Rate (%)", min_value=0.0, step=0.01, key="catering_airport_fee_us"
            )

        submitted = st.form_submit_button("Submit")

    if submitted:
        st.session_state.catering_items = items_df
        if region == "Canada":
            breakdown = compute_catering_canada(items_df, delivery_fees, tax_rate, airport_fee_rate)
        else:
            breakdown = compute_catering_us(items_df, delivery_fees, tax_rate, airport_fee_rate)
        st.session_state.catering_result = {
            "region": region,
            "breakdown": breakdown,
            "items": items_df,
        }

    result = st.session_state.catering_result
    if result:
        breakdown: CateringBreakdown = result["breakdown"]
        st.markdown("### Breakdown")
        line_items = result["items"].copy()
        line_items["Unit Price"] = _coerce_numeric(line_items["Unit Price"])
        line_items["Qty"] = _coerce_numeric(line_items["Qty"])
        line_items["Line Total"] = line_items["Unit Price"] * line_items["Qty"]
        line_items_display = line_items.copy()
        for column in ["Unit Price", "Line Total"]:
            line_items_display[column] = line_items_display[column].map(_currency)
        line_items_display["Qty"] = line_items_display["Qty"].map(lambda value: f"{value:g}")
        st.dataframe(line_items_display, use_container_width=True, hide_index=True)
        display_rows = [
            ("Items Total", breakdown.items_total),
        ]
        if result["region"] == "Canada":
            display_rows.append(("Delivery Fees", breakdown.delivery_fees))
        else:
            display_rows.append(("Delivery/Airport Fees", breakdown.delivery_fees))
        display_rows.extend(
            [
                ("Airport Fee", breakdown.airport_fee),
                ("Taxes", breakdown.taxes),
                ("AirSprint Fee", breakdown.air_sprint_fee),
                ("Grand Total", breakdown.grand_total),
            ]
        )
        breakdown_df = pd.DataFrame(display_rows, columns=["Line Item", "Amount"])
        breakdown_df["Amount"] = breakdown_df["Amount"].map(_currency)
        st.dataframe(breakdown_df, use_container_width=True, hide_index=True)

        if st.button("Export breakdown (CSV)", key="export_catering"):
            st.download_button(
                "Download catering breakdown",
                breakdown_df.to_csv(index=False).encode("utf-8"),
                file_name="catering_breakdown.csv",
                mime="text/csv",
            )

with gt_tab:
    st.subheader("GT Calculator")

    if st.button("Add charge"):
        st.session_state.gt_additional = pd.concat(
            [st.session_state.gt_additional, _default_additional_rows().iloc[:1]],
            ignore_index=True,
        )

    with st.form("gt_form"):
        region = st.selectbox("Region", ["Canada", "US/International"], key="gt_region")
        base_cost = st.number_input("Base GT Cost ($)", min_value=0.0, step=0.01, key="gt_base")
        cc_rate = st.number_input("Credit Card Fee (%)", min_value=0.0, max_value=100.0, step=0.01, key="gt_cc")

        if region == "Canada":
            airport_code = st.text_input("Airport (ICAO/IATA)", key="gt_airport_code")
            province_code = _lookup_province_code(airport_code)
            if airport_code and not province_code:
                st.warning("Province not found for that airport code. Please select the tax rate manually.")
            if province_code:
                st.caption(f"Province detected: {province_code}")
                tax_rate = st.number_input(
                    "Tax Rate (%)",
                    value=PROVINCE_TAX_RATES[province_code],
                    disabled=True,
                    key="gt_tax_auto",
                )
            else:
                tax_selection = st.selectbox(
                    "Tax Rate (%)",
                    options=_tax_rate_options(),
                    key="gt_tax_selection",
                )
                tax_rate = _tax_rate_from_selection(tax_selection)
            gratuity_rate = st.number_input(
                "Gratuity (%)", min_value=0.0, max_value=100.0, step=0.01, value=20.0, key="gt_gratuity"
            )
            fuel_rate = st.number_input(
                "Fuel Surcharge (%)", min_value=0.0, max_value=100.0, step=0.01, key="gt_fuel"
            )
            additional_df = pd.DataFrame()
        else:
            gratuity_rate = st.number_input(
                "Gratuity (%)", min_value=0.0, max_value=100.0, step=0.01, value=20.0, key="gt_gratuity_us"
            )
            additional_df = _ensure_dataframe(
                st.session_state.gt_additional,
                ["Label", "Type", "Value"],
                DEFAULT_ADDITIONAL_ROWS,
            )
            additional_df = st.data_editor(
                additional_df,
                key="gt_additional_editor",
                use_container_width=True,
                column_config={
                    "Label": st.column_config.TextColumn("Label"),
                    "Type": st.column_config.SelectboxColumn("Type", options=["%", "$"] ),
                    "Value": st.column_config.NumberColumn("Value", min_value=0.0, step=0.01),
                },
            )
            tax_rate = 0.0
            fuel_rate = 0.0

        submitted = st.form_submit_button("Submit")

    if submitted:
        if region == "Canada":
            breakdown = compute_gt_canada(base_cost, cc_rate, tax_rate, gratuity_rate, fuel_rate)
        else:
            st.session_state.gt_additional = additional_df
            breakdown = compute_gt_us(base_cost, cc_rate, gratuity_rate, additional_df)
        st.session_state.gt_result = {
            "region": region,
            "breakdown": breakdown,
        }

    result = st.session_state.gt_result
    if result:
        breakdown: GTBreakdown = result["breakdown"]
        st.markdown("### Breakdown")
        rows = [
            ("Base", breakdown.base),
            ("Gratuity", breakdown.gratuity),
        ]
        if result["region"] != "Canada":
            for label, amount in breakdown.additional_lines:
                rows.append((label, amount))
            rows.append(("Additional Charges Total", breakdown.additional_total))
        else:
            if breakdown.additional_lines:
                for label, amount in breakdown.additional_lines:
                    rows.append((label, amount))
        rows.append(("Credit Card Fee", breakdown.credit_card_fee))
        rows.append(("AirSprint Fee", breakdown.air_sprint_fee))
        if result["region"] == "Canada":
            rows.append(("Taxes", breakdown.taxes))
        rows.append(("Grand Total", breakdown.grand_total))

        breakdown_df = pd.DataFrame(rows, columns=["Line Item", "Amount"])
        breakdown_df["Amount"] = breakdown_df["Amount"].map(_currency)
        st.dataframe(breakdown_df, use_container_width=True, hide_index=True)

        if st.button("Export breakdown (CSV)", key="export_gt"):
            st.download_button(
                "Download GT breakdown",
                breakdown_df.to_csv(index=False).encode("utf-8"),
                file_name="gt_breakdown.csv",
                mime="text/csv",
            )

    if region == "US/International":
        if any(additional_df["Value"].clip(lower=0).gt(100)):
            st.warning("Some additional charge percentages are above 100%.")
