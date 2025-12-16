from __future__ import annotations

from textwrap import dedent

import pandas as pd
import streamlit as st

from diagnostics_utils import collect_fd_usage
from Home import configure_page, password_gate, render_sidebar


_PAGE_TITLE = "System Diagnostics"


def _render_fd_summary() -> None:
    usage = collect_fd_usage()

    st.metric(
        "Open file descriptors",
        f"{usage.open_total}",
        delta=f"{usage.usage_pct:.1f}% of soft limit",
    )

    st.caption(
        f"Soft limit: {usage.soft_limit:,} • Hard limit: {usage.hard_limit:,}"
    )

    st.progress(min(1.0, usage.usage_pct / 100))

    if usage.usage_pct >= 80:
        st.warning(
            "You are approaching the soft limit. Consider raising the limit or reducing the number of open files/connections."
        )

    counts = sorted(usage.counts_by_type.items(), key=lambda kv: kv[0])
    if counts:
        st.subheader("By descriptor type")
        df = pd.DataFrame(counts, columns=["Type", "Count"])
        st.dataframe(df, width="stretch", hide_index=True)
    else:
        st.info("No open descriptors detected from /proc/self/fd.")

    if usage.top_targets:
        st.subheader("Noisiest handles")
        st.caption("Top 20 descriptor targets by frequency.")
        targets_df = pd.DataFrame(
            usage.top_targets, columns=["Target", "Count"]
        )
        st.dataframe(targets_df, width="stretch", hide_index=True)
    else:
        st.info("Descriptor targets could not be determined.")

    st.button("🔄 Refresh", on_click=st.rerun)


def _render_preventative_steps() -> None:
    st.subheader("How to keep descriptor counts under control")
    st.markdown(
        dedent(
            """
            - **Close resources promptly:** Ensure HTTP responses, file handles, and database cursors are closed.
            - **Time out idle work:** Configure read/write/idleness timeouts for sockets and WebSockets.
            - **Limit watchers/subprocesses:** Avoid opening many concurrent `tail -F` or polling tasks.
            - **Graceful shutdowns:** Stop accepting new work before restarts so descriptors are released cleanly.
            - **Raise limits when justified:** Increase `ulimit -n`/`LimitNOFILE` for the service user if workload requires it.
            """
        )
    )


def main() -> None:
    configure_page(page_title=_PAGE_TITLE)
    password_gate()
    render_sidebar()

    st.title(_PAGE_TITLE)

    _render_fd_summary()
    st.divider()
    _render_preventative_steps()


if __name__ == "__main__":
    main()
