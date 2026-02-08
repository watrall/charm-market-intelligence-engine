import streamlit as st

# Reuse helpers and the guided flow from the main dashboard module
from dashboard import app as explore_app
from dashboard.header import render_header

st.set_page_config(
    page_title="CHARM",
    layout="wide",
    initial_sidebar_state="collapsed",
)


def main():
    explore_app.ensure_lucide()

    mode = explore_app.app_mode()
    proc_dir = explore_app.processed_dir()

    render_header(proc_dir)

    st.title("Ingest & Analyze")
    st.caption("Guided flow to ingest data and run the pipeline. Explore remains a separate surface.")
    st.page_link("app.py", label="Go to Explore", icon="↩️")

    explore_app._wizard_header(mode, proc_dir)
    st.divider()
    steps = ["Start", "Ingest", "Configure", "Run", "Results"]
    idx = explore_app._wizard_stepper(steps)

    # Nav guards: enforce sequential flow but allow revisits of completed steps
    if idx > 0 and not st.session_state.get("wizard_started"):
        st.info("Click Start Ingest & Analyze to begin.")
        return

    if idx == 0:
        st.subheader("Start")
        st.write("This flow guides you through ingestion, processing, and visualization.")
        st.write("Click 'Start Ingest & Analyze' to begin, then follow the numbered steps.")
        st.caption("You can return to any completed step to make changes.")
        if "wizard_done" in st.session_state:
            st.session_state["wizard_done"]["Start"] = True
    elif idx == 1:
        explore_app._render_ingest_step(mode)
    elif idx == 2:
        explore_app._render_config_step(mode)
    elif idx == 3:
        explore_app._render_run_step(mode)
    else:
        explore_app._render_results_step(proc_dir)


if __name__ == "__main__":
    main()
