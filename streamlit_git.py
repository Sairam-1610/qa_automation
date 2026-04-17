import streamlit as st
import pandas as pd
import requests
import time
import json

# -------------------------------------------------------------------
# CONFIGURATION (replace with st.secrets in production)
# -------------------------------------------------------------------
DATABRICKS_INSTANCE = "https://dbc-e124ec40-fa61.cloud.databricks.com"
TOKEN = "dapi205af0a0551b72580fa4055f98c5be20"
JOB_ID = "408448156916986"

# -------------------------------
# Page Configuration
# -------------------------------
st.set_page_config(
    page_title="End-to-End AI QA",
    layout="wide"
)

# -------------------------------
# Header
# -------------------------------
st.title("End‑to‑End AI QA for Ingestion Pipelines")

# -------------------------------
# Main Layout
# -------------------------------
left_col, right_col = st.columns([3.5, 1.5], gap="medium")

# ============================================================================
# LEFT PANEL
# ============================================================================
with left_col:

    with st.container(border=True):
        st.subheader("Ingestion Configuration")

        st.selectbox(
            "Select STM Location",
            ["Select", "STM_Location_1", "STM_Location_2"],
            key="stm_location"
        )

        stm_file_path = st.text_input(
            "Files Location (parquet, csv, etc)",
            placeholder="Enter STM file path",
            key="files_location"
        )

        # ✅ THIS is the ONLY trigger button
        summary_clicked = st.button("Summary", key="summary_btn")

    with st.container(border=True):
        st.subheader("Quality Assurance")

        st.button("Run All Validations", use_container_width=True)

        st.markdown("**Structure**")
        col1, col2 = st.columns(2)
        with col1:
            st.button("Test Case Generator", use_container_width=True)
        with col2:
            st.button("Test Case Validation", use_container_width=True)

        st.markdown("**SCD**")
        col3, col4 = st.columns(2)
        with col3:
            st.button("Test Case Generator", use_container_width=True)
        with col4:
            st.button("Test Case Validation", use_container_width=True)

# ============================================================================
# RIGHT PANEL – SUMMARY VIEWER
# ============================================================================
with right_col:

    with st.container(border=True):
        st.subheader("Summary Viewer")

        st.markdown("**Pipeline Name**")
        st.write("STM Name")

        st.markdown("**Source**")
        st.write("Schema Name · Table Name")

        st.markdown("**Target**")
        st.write("Schema Name · Table Name")

        st.divider()

        # Initialize summary storage in session state
        if "df_summary" not in st.session_state:
            st.session_state.df_summary = pd.DataFrame(
                columns=["Category", "Details"]
            )

        # --------------------------------------------------
        # RUN NOTEBOOK WHEN SUMMARY BUTTON IS CLICKED
        # --------------------------------------------------
        if summary_clicked:

            headers = {
                "Authorization": f"Bearer {TOKEN}"
            }

            payload = {
                "job_id": JOB_ID,
                "notebook_params": {
                    "STM_FILE_PATH": stm_file_path
                }
            }

            try:
                # 1️⃣ Trigger Databricks Job
                run_resp = requests.post(
                    f"{DATABRICKS_INSTANCE}/api/2.2/jobs/run-now",
                    json=payload,
                    headers=headers,
                    timeout=30
                )

                if run_resp.status_code != 200:
                    st.error("Failed to trigger Databricks job")
                    st.write(run_resp.text)
                else:
                    run_id = run_resp.json().get("run_id")

                    if not run_id:
                        st.error("Databricks did not return run_id")
                    else:
                        # 2️⃣ Fetch job result ONCE
                        status_resp = requests.get(
                            f"{DATABRICKS_INSTANCE}/api/2.2/jobs/runs/get",
                            headers=headers,
                            params={"run_id": run_id},
                            timeout=30
                        )

                        if status_resp.status_code != 200:
                            st.error("Failed to fetch job result")
                            st.write(status_resp.text)
                        else:
                            status_data = status_resp.json()
                            state = status_data.get("state", {})

                            if state.get("result_state") != "SUCCESS":
                                msg = state.get(
                                    "state_message",
                                    "Notebook failed"
                                )
                                st.error(msg)
                            else:
                                notebook_output = (
                                    status_data
                                    .get("notebook_output", {})
                                    .get("result")
                                )

                                if not notebook_output:
                                    st.error(
                                        "Notebook returned no output. "
                                        "Ensure dbutils.notebook.exit(json) is used."
                                    )
                                else:
                                    records = json.loads(notebook_output)
                                    st.session_state.df_summary = pd.DataFrame(records)
                                    st.success("Summary loaded")

            except requests.exceptions.Timeout:
                st.error("Databricks request timed out")

            except json.JSONDecodeError:
                st.error("Invalid JSON returned from notebook")

            except Exception as e:
                st.error(f"Unexpected error: {e}")

        # --------------------------------------------------
        # SCROLLABLE SUMMARY TABLE
        # --------------------------------------------------
        st.dataframe(
            st.session_state.df_summary,
            use_container_width=True,
            height=260,
            hide_index=True
        )
