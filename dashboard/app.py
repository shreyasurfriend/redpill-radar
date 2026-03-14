import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

API_BASE = "http://localhost:8000"
CONTENT_URL = f"{API_BASE}/api/v1/content"
CATEGORIES_URL = f"{API_BASE}/api/v1/categories"
STATUS_URL = f"{API_BASE}/api/v1/content/{{id}}/status"
REPROCESS_URL = f"{API_BASE}/api/v1/content/reprocess"

REPO_ROOT = Path(__file__).resolve().parent.parent
COLLECT_DIR = REPO_ROOT / "collect"
PYTHON_EXE = str(REPO_ROOT / "venv" / "Scripts" / "python.exe")

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="RedPill Radar - Pipeline Dashboard",
    page_icon="🔍",
    layout="wide",
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def derive_stage(row: dict) -> str:
    if row.get("analysis_status") == "failed":
        return "Failed"
    if row.get("is_processed"):
        return "Rebutted"
    if row.get("analysis_status") == "completed":
        return "Analysed"
    return "Collected"


STAGE_ORDER = ["Collected", "Analysed", "Rebutted", "Failed"]
STAGE_COLORS = {
    "Collected": "#3498db",
    "Analysed": "#f39c12",
    "Rebutted": "#2ecc71",
    "Failed": "#e74c3c",
}


@st.cache_data(ttl=5)
def fetch_content(page: int = 1, limit: int = 100) -> list[dict]:
    try:
        resp = requests.get(CONTENT_URL, params={"page": page, "limit": limit}, timeout=5)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as e:
        st.error(f"Failed to fetch content from Analyse API: {e}")
        return []


@st.cache_data(ttl=30)
def fetch_categories() -> list[dict]:
    try:
        resp = requests.get(CATEGORIES_URL, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return []


def build_dataframe(items: list[dict]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)
    df["stage"] = df.apply(derive_stage, axis=1)

    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    subcats_col = "harmful_subcategories"
    if subcats_col in df.columns:
        df[subcats_col] = df[subcats_col].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else (x or "")
        )

    confidence_col = "labels"
    if confidence_col in df.columns:
        df["confidence"] = df[confidence_col].apply(
            lambda x: x.get("confidence", None) if isinstance(x, dict) else None
        )
        df["reasoning"] = df[confidence_col].apply(
            lambda x: x.get("reasoning", "") if isinstance(x, dict) else ""
        )

    return df


def stage_badge(stage: str) -> str:
    color = STAGE_COLORS.get(stage, "#95a5a6")
    return (
        f'<span style="background-color:{color};color:white;padding:2px 10px;'
        f'border-radius:12px;font-size:0.85em;font-weight:600;">{stage}</span>'
    )


# ── Pipeline actions ─────────────────────────────────────────────────────────


def run_collect() -> tuple[bool, str]:
    """Run the collect/main.py scraper as a subprocess."""
    try:
        result = subprocess.run(
            [PYTHON_EXE, "main.py"],
            cwd=str(COLLECT_DIR),
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Collect timed out after 120 seconds."
    except Exception as e:
        return False, str(e)


def run_generate_test_data() -> tuple[bool, str]:
    """Generate dummy test data and push to Analyse API (no Twitter needed)."""
    try:
        result = subprocess.run(
            [PYTHON_EXE, "generate_test_data.py"],
            cwd=str(COLLECT_DIR),
            capture_output=True,
            text=True,
            timeout=60,
        )
        output = result.stdout + result.stderr
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Test data generation timed out."
    except Exception as e:
        return False, str(e)


def run_rebutt() -> tuple[int, int, str]:
    """
    Simulate rebutt: mark all analysed-but-unprocessed items as processed
    via the PATCH status endpoint, adding a review comment.
    """
    items = fetch_content.__wrapped__(page=1, limit=100)
    analysed = [
        i for i in items
        if i.get("analysis_status") == "completed" and not i.get("is_processed")
    ]

    if not analysed:
        return 0, 0, "No analysed items to process."

    success = 0
    failed = 0
    for item in analysed:
        content_type = item.get("content_type", "unknown")
        subcats = item.get("harmful_subcategories") or []
        if isinstance(subcats, str):
            subcats = [s.strip() for s in subcats.split(",") if s.strip()]

        if content_type == "harmful":
            comment = (
                f"Flagged as harmful content. "
                f"Subcategories: {', '.join(subcats) if subcats else 'none identified'}. "
                f"Recommended for review and platform reporting."
            )
        else:
            comment = "Content reviewed and deemed safe. No action required."

        try:
            resp = requests.patch(
                STATUS_URL.format(id=item["id"]),
                json={"is_processed": True, "review_comment": comment},
                timeout=5,
            )
            resp.raise_for_status()
            success += 1
        except Exception:
            failed += 1

    summary = f"Processed {success} item(s)"
    if failed:
        summary += f", {failed} failed"
    return success, failed, summary


def run_analyse() -> tuple[bool, str]:
    """Trigger Analyse API to run Groq analysis on all pending (Collected) items."""
    try:
        resp = requests.post(REPROCESS_URL, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return True, data.get("message", "Reprocessing started.")
    except requests.exceptions.RequestException as e:
        return False, str(e)


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("Controls")

st.sidebar.subheader("Pipeline Actions")

if st.sidebar.button("Generate Test Data", type="primary", use_container_width=True):
    with st.spinner("Generating dummy tweets..."):
        ok, output = run_generate_test_data()
    if ok:
        st.sidebar.success("Test data generated.")
    else:
        st.sidebar.error("Test data generation failed.")
    with st.sidebar.expander("Output"):
        st.code(output, language="text")
    fetch_content.clear()
    st.rerun()

if st.sidebar.button("Run Full Pipeline", use_container_width=True):
    st.sidebar.info("Step 1/3: Generating test data...")
    ok, output = run_generate_test_data()
    if ok:
        st.sidebar.success("Test data generated.")
    else:
        st.sidebar.error("Test data failed.")
    with st.sidebar.expander("Step 1 output"):
        st.code(output, language="text")

    st.sidebar.info("Step 2/3: Running Analyse...")
    ok, msg = run_analyse()
    if ok:
        st.sidebar.success("Analyse triggered.")
    else:
        st.sidebar.error(f"Analyse failed: {msg}")
    time.sleep(5)

    st.sidebar.info("Step 3/3: Running Rebutt...")
    fetch_content.clear()
    s, f, msg = run_rebutt()
    st.sidebar.success(f"Rebutt done: {msg}")

    fetch_content.clear()
    st.rerun()

col_btn1, col_btn2, col_btn3 = st.sidebar.columns(3)
with col_btn1:
    if st.button("Collect", use_container_width=True):
        with st.spinner("Running collector..."):
            ok, output = run_collect()
        if ok:
            st.sidebar.success("Collect completed.")
        else:
            st.sidebar.error("Collect had issues.")
        with st.sidebar.expander("Collect output"):
            st.code(output, language="text")
        fetch_content.clear()
        st.rerun()

with col_btn2:
    if st.button("Analyse", use_container_width=True):
        with st.spinner("Running analyse..."):
            ok, msg = run_analyse()
        if ok:
            st.sidebar.success(msg)
        else:
            st.sidebar.error(f"Analyse failed: {msg}")
        fetch_content.clear()
        st.rerun()

with col_btn3:
    if st.button("Rebutt", use_container_width=True):
        with st.spinner("Processing with Rebutt..."):
            s, f, msg = run_rebutt()
        st.sidebar.success(msg)
        fetch_content.clear()
        st.rerun()

st.sidebar.divider()

auto_refresh = st.sidebar.toggle("Auto-refresh", value=False)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)

st.sidebar.divider()
st.sidebar.subheader("Filters")
filter_stage = st.sidebar.multiselect("Stage", STAGE_ORDER, default=STAGE_ORDER)
filter_content_type = st.sidebar.multiselect(
    "Content Type", ["safe", "harmful"], default=["safe", "harmful"]
)

# ── Fetch data ───────────────────────────────────────────────────────────────

items = fetch_content()
df = build_dataframe(items)

# ── Header ───────────────────────────────────────────────────────────────────

st.title("RedPill Radar")
st.caption("Pipeline Dashboard  --  Collect → Analyse → Rebutt")

# ── Pipeline metrics ─────────────────────────────────────────────────────────

if df.empty:
    st.info("No content in the database yet. Run the **collect** module to ingest tweets.")
else:
    stage_counts = df["stage"].value_counts()

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", len(df))
    col2.metric("Collected", int(stage_counts.get("Collected", 0)))
    col3.metric("Analysed", int(stage_counts.get("Analysed", 0)))
    col4.metric("Rebutted", int(stage_counts.get("Rebutted", 0)))
    col5.metric("Failed", int(stage_counts.get("Failed", 0)))

    st.divider()

    # ── Tabs ─────────────────────────────────────────────────────────────

    tab_table, tab_issues, tab_charts = st.tabs(
        ["Pipeline Table", "Issues & Errors", "Category Breakdown"]
    )

    # ── Tab 1: Pipeline Table ────────────────────────────────────────────

    with tab_table:
        filtered = df[
            df["stage"].isin(filter_stage)
            & df["content_type"].isin(filter_content_type + [None, ""])
        ].sort_values("created_at", ascending=False, na_position="last")

        if filtered.empty:
            st.warning("No content matches the current filters.")
        else:
            for _, row in filtered.iterrows():
                content_preview = (
                    row["content_text"][:120] + "..."
                    if len(str(row["content_text"])) > 120
                    else row["content_text"]
                )

                header_cols = st.columns([0.5, 3, 1, 1, 1])
                with header_cols[0]:
                    st.markdown(stage_badge(row["stage"]), unsafe_allow_html=True)
                with header_cols[1]:
                    st.markdown(
                        f"**{content_preview}**",
                    )
                with header_cols[2]:
                    ct = row.get("content_type") or "pending"
                    st.caption(f"Type: **{ct}**")
                with header_cols[3]:
                    conf = row.get("confidence")
                    st.caption(f"Confidence: **{conf:.0%}**" if conf else "Confidence: --")
                with header_cols[4]:
                    ts = row.get("created_at")
                    st.caption(
                        f"{ts.strftime('%Y-%m-%d %H:%M')}" if pd.notna(ts) else "--"
                    )

                with st.expander(f"Details  --  Twitter ID: {row['twitter_id']}"):
                    detail_col1, detail_col2 = st.columns(2)

                    with detail_col1:
                        st.markdown("**Full Content**")
                        st.text_area(
                            "Content",
                            row["content_text"],
                            height=100,
                            disabled=True,
                            label_visibility="collapsed",
                        )
                        st.markdown(f"**Internal ID:** `{row['id']}`")
                        st.markdown(f"**Twitter ID:** `{row['twitter_id']}`")
                        st.markdown(f"**Age Category:** {row.get('age_category') or '--'}")
                        st.markdown(
                            f"**Subcategories:** {row.get('harmful_subcategories') or '--'}"
                        )

                    with detail_col2:
                        st.markdown("**Analysis**")
                        st.markdown(f"**Status:** {row.get('analysis_status')}")
                        st.markdown(f"**Reasoning:** {row.get('reasoning') or '--'}")

                        if row.get("review_comment"):
                            st.markdown("**Review Comment (Rebutt)**")
                            st.info(row["review_comment"])

                        raw = row.get("raw_analysis")
                        if isinstance(raw, dict):
                            st.markdown("**Raw Groq Response**")
                            st.json(raw)

                    history = row.get("processing_history")
                    if history and isinstance(history, list) and len(history) > 0:
                        st.markdown("**Processing History**")
                        for entry in history:
                            ts_str = entry.get("timestamp", "")
                            action = entry.get("action", "")
                            comment = entry.get("comment") or ""
                            st.caption(f"`{ts_str}` -- **{action}** {comment}")

                st.markdown("---")

    # ── Tab 2: Issues & Errors ───────────────────────────────────────────

    with tab_issues:
        failed = df[df["stage"] == "Failed"]
        if failed.empty:
            st.success("No failed items.")
        else:
            st.error(f"{len(failed)} item(s) failed analysis")
            for _, row in failed.iterrows():
                with st.expander(
                    f"FAILED -- Twitter ID: {row['twitter_id']}"
                ):
                    st.text_area(
                        "Content",
                        row["content_text"],
                        height=80,
                        disabled=True,
                        label_visibility="collapsed",
                    )
                    raw = row.get("raw_analysis")
                    if isinstance(raw, dict):
                        st.json(raw)

        st.divider()

        now = datetime.now(timezone.utc)
        pending = df[df["stage"] == "Collected"].copy()
        if not pending.empty and "created_at" in pending.columns:
            pending["age_minutes"] = pending["created_at"].apply(
                lambda x: (now - x.to_pydatetime().replace(tzinfo=timezone.utc)).total_seconds() / 60
                if pd.notna(x) and x.tzinfo is None
                else (now - x.to_pydatetime()).total_seconds() / 60
                if pd.notna(x)
                else 0
            )
            stuck = pending[pending["age_minutes"] > 5]
            if not stuck.empty:
                st.warning(
                    f"{len(stuck)} item(s) stuck in 'Collected' for over 5 minutes"
                )
                for _, row in stuck.iterrows():
                    st.caption(
                        f"Twitter ID: `{row['twitter_id']}` -- "
                        f"pending for {row['age_minutes']:.0f} min"
                    )
            else:
                st.success("No items stuck in pending.")
        else:
            st.success("No pending items.")

    # ── Tab 3: Category Breakdown ────────────────────────────────────────

    with tab_charts:
        analysed = df[df["analysis_status"] == "completed"]
        if analysed.empty:
            st.info("No analysed content yet for charts.")
        else:
            chart_col1, chart_col2 = st.columns(2)

            with chart_col1:
                st.markdown("### Content Type Distribution")
                type_counts = analysed["content_type"].value_counts()
                st.bar_chart(type_counts)

            with chart_col2:
                st.markdown("### Age Category Distribution")
                age_counts = analysed["age_category"].value_counts()
                st.bar_chart(age_counts)

            st.markdown("### Harmful Subcategory Frequency")
            all_subcats = []
            for val in df["harmful_subcategories"]:
                if val and isinstance(val, str):
                    for s in val.split(", "):
                        if s.strip():
                            all_subcats.append(s.strip())
            if all_subcats:
                subcat_series = pd.Series(all_subcats).value_counts()
                st.bar_chart(subcat_series)
            else:
                st.caption("No harmful subcategories found.")

            st.markdown("### Pipeline Stage Distribution")
            stage_series = df["stage"].value_counts().reindex(STAGE_ORDER, fill_value=0)
            st.bar_chart(stage_series)

# ── Auto-refresh ─────────────────────────────────────────────────────────────

if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
