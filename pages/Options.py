"""Streamlit entrypoint for the Options solver prototype."""

from core.neg_scheduler.contracts import Flight, Tail  # noqa: F401
from core.neg_scheduler.model import LeverPolicy, NegotiationScheduler  # noqa: F401

from apps.negotiation_optimizer.app import render_page

render_page()
