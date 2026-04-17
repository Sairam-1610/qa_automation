import streamlit as st
import pandas as pd

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

    # ===============================
    # INGESTION CONFIGURATION
    # ===============================
    with st.container(border=True):
        st.subheader("Ingestion Configuration")

        st.selectbox(
            "Select STM Location",
            ["Select", "STM_Location_1", "STM_Location_2"],
            key="stm_location"
        )

        st.text_input(
            "Files Location (parquet, csv, etc)",
            placeholder="Enter file or folder path",
            key="files_location"
        )

        st.button("Summary", key="summary_btn")

    # ===============================
    # QUALITY ASSURANCE
    # ===============================
    with st.container(border=True):
        st.subheader("Quality Assurance")

        st.button(
            "Run All Validations",
            key="run_all_validations",
            use_container_width=True
        )

        st.markdown("**Structure**")
        col1, col2 = st.columns(2)

        with col1:
            st.button(
                "Test Case Generator",
                key="structure_generator",
                use_container_width=True
            )

        with col2:
            st.button(
                "Test Case Validation",
                key="structure_validation",
                use_container_width=True
            )

        st.markdown("**SCD**")
        col3, col4 = st.columns(2)

        with col3:
            st.button(
                "Test Case Generator",
                key="scd_generator",
                use_container_width=True
            )

        with col4:
            st.button(
                "Test Case Validation",
                key="scd_validation",
                use_container_width=True
            )

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
        # ------------------------------------------------------------------
        # DATABRICKS METADATA TABLE PLACEHOLDER
        #
        # EXPECTED FORMAT FROM DATABRICKS:
        # ------------------------------------------------------------------
        # A Spark / SQL result with EXACTLY these two columns:
        #
        #   Category : STRING
        #   Details  : STRING
        #
        # Each row represents one metadata attribute, e.g.:
        #   - Source Name
        #   - Target Table
        #   - Load Type
        #   - Load Strategy
        #   - Total Column Count
        #   - Primary Keys
        #   - PII Present
        #   - Temporal Columns
        #
        # INTEGRATION STEPS (to be added later):
        # ------------------------------------------------------------------
        # 1. Query metadata from Azure Databricks (SQL or Spark)
        # 2. Result must return (Category, Details)
        # 3. Convert Spark DataFrame → Pandas:
        #
        #       df_summary = spark_df.toPandas()
        #
        # 4. Replace the empty DataFrame below with df_summary
        # ------------------------------------------------------------------

        # Empty dataframe placeholder (keeps UI layout intact)
        df_summary = pd.DataFrame(columns=["Category", "Details"])

        # Scrollable table
        st.dataframe(
            df_summary,
            use_container_width=True,
            height=260,     # Enables vertical scrolling
            hide_index=True
        )
