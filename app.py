import zipfile
from io import BytesIO

import pandas as pd
import streamlit as st

from transform import process_cost_files, process_revenue_files

st.set_page_config(page_title="Adaptive Upload Tool", layout="wide")

st.title("Adaptive Upload Tool")
st.write("Use the sidebar to switch between Adaptive Cost Upload and Adaptive Revenue Demand Upload.")

page = st.sidebar.radio(
    "Select section",
    ["Adaptive Cost Upload", "Adaptive Revenue Demand Upload"],
)

if page == "Adaptive Cost Upload":
    st.header("Adaptive Cost Upload")

    with st.sidebar:
        st.subheader("Required files for Cost Upload")
        st.markdown(
            """
            Upload:
            1. Raw JEDI report (.xlsx)
            2. Vendor mapping (.csv)
            3. Accounts (.csv)
            """
        )

    raw_jedi_file = st.file_uploader(
        "Upload raw JEDI report",
        type=["xlsx"],
        key="cost_raw_jedi_file",
    )
    vendor_mapping_file = st.file_uploader(
        "Upload vendor mapping CSV",
        type=["csv"],
        key="cost_vendor_mapping_file",
    )
    accounts_file = st.file_uploader(
        "Upload Accounts CSV",
        type=["csv"],
        key="cost_accounts_file",
    )

    if raw_jedi_file and vendor_mapping_file and accounts_file:
        if st.button("Generate cost output"):
            try:
                with st.spinner("Processing cost files..."):
                    result = process_cost_files(
                        raw_jedi_file=raw_jedi_file,
                        vendor_mapping_file=vendor_mapping_file,
                        accounts_file=accounts_file,
                    )

                cos_operating_expenses_df = result["output"]
                remaining_add_vendor_codes = result["remaining_add_vendor_codes"]
                new_mappings_df = result["new_mappings_df"]
                updated_vendor_mapping_df = result["vendor_mapping"]

                st.success("Cost processing complete.")

                st.subheader("Final output preview")
                st.dataframe(cos_operating_expenses_df.head(50), use_container_width=True)

                st.subheader("Summary")
                st.write(f"Final output rows: {len(cos_operating_expenses_df)}")
                st.write(f"New vendor mappings added: {len(new_mappings_df)}")
                st.write(f"Remaining unresolved vendor rows: {len(remaining_add_vendor_codes)}")

                st.subheader("Updated Vendor Mapping")
                st.dataframe(updated_vendor_mapping_df.head(50), use_container_width=True)

                if not new_mappings_df.empty:
                    st.subheader("New Vendor Mappings Added")
                    st.dataframe(new_mappings_df, use_container_width=True)

                if not remaining_add_vendor_codes.empty:
                    st.subheader("Remaining ADD VENDOR CODES Rows")
                    cols_to_show = [
                        c
                        for c in ["PARTY_NAME", "JOURNAL_LINE_DESCRIPTION", "USD_AMOUNT"]
                        if c in remaining_add_vendor_codes.columns
                    ]
                    st.dataframe(
                        remaining_add_vendor_codes[cols_to_show].head(50),
                        use_container_width=True,
                    )

                output_buffer = BytesIO()
                with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
                    cos_operating_expenses_df.to_excel(
                        writer,
                        sheet_name="COS & Operating Expenses",
                        index=False,
                    )

                    updated_vendor_mapping_df.to_excel(
                        writer,
                        sheet_name="Updated Vendor Mapping",
                        index=False,
                    )

                    if not new_mappings_df.empty:
                        new_mappings_df.to_excel(
                            writer,
                            sheet_name="New Vendor Mappings",
                            index=False,
                        )

                    if not remaining_add_vendor_codes.empty:
                        remaining_add_vendor_codes.to_excel(
                            writer,
                            sheet_name="Unresolved Vendor Codes",
                            index=False,
                        )

                output_buffer.seek(0)

                st.download_button(
                    label="Download Cost Output Excel File",
                    data=output_buffer.getvalue(),
                    file_name="COS_Operating_Expenses_Output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

                vendor_csv = updated_vendor_mapping_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download Updated Vendor Mapping (CSV)",
                    data=vendor_csv,
                    file_name="updated_vendor_mapping.csv",
                    mime="text/csv",
                )

            except Exception as e:
                st.error(f"Error while processing cost files: {e}")
    else:
        st.info("Please upload all three required cost files.")

elif page == "Adaptive Revenue Demand Upload":
    st.header("Adaptive Revenue Demand Upload")

    with st.sidebar:
        st.subheader("Required files for Revenue Demand Upload")
        st.markdown(
            """
            Upload:
            1. Instructions Excel (.xlsx) with 3 sheets
            2. Demand data CSV (.csv)
            3. Demand ID CSV (.csv)
            """
        )

    instructions_file = st.file_uploader(
        "Upload instructions Excel",
        type=["xlsx"],
        key="rev_instructions_file",
    )
    demand_data_file = st.file_uploader(
        "Upload demand data CSV",
        type=["csv"],
        key="rev_demand_data_file",
    )
    demand_id_file = st.file_uploader(
        "Upload demand ID CSV",
        type=["csv"],
        key="rev_demand_id_file",
    )

    if instructions_file and demand_data_file and demand_id_file:
        if st.button("Generate revenue demand output"):
            try:
                with st.spinner("Processing revenue demand files..."):
                    result = process_revenue_files(
                        instructions_file=instructions_file,
                        demand_data_file=demand_data_file,
                        demand_id_file=demand_id_file,
                    )

                known_demand_ids_df = result["known_demand_ids_df"]
                new_mappings_df = result["new_mappings_df"]
                generated_reports = result["generated_reports"]
                month_label = result["month_label"]

                st.success("Revenue demand processing complete.")

                st.subheader("Summary")
                st.write(f"Generated report files: {len(generated_reports)}")
                st.write(f"Known demand IDs in uploaded demand ID file: {len(known_demand_ids_df)}")
                st.write(f"New demand_id + dsp_name pairs found: {len(new_mappings_df)}")
                st.write(f"Output month label: {month_label}")

                if not new_mappings_df.empty:
                    st.subheader("New demand_id / dsp_name pairs found")
                    st.dataframe(new_mappings_df, use_container_width=True)

                    new_mapping_csv = new_mappings_df.to_csv(index=False).encode("utf-8")
                    st.download_button(
                        label="Download New demand_id / dsp_name Pairs (CSV)",
                        data=new_mapping_csv,
                        file_name="new_demand_id_dsp_name_pairs.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No new demand_id / dsp_name pairs were found.")

                if generated_reports:
                    st.subheader("Generated Revenue Files")
                    for report_name, report_bytes in generated_reports.items():
                        st.download_button(
                            label=f"Download {report_name}",
                            data=report_bytes,
                            file_name=report_name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key=f"download_{report_name}",
                        )

                    zip_buffer = BytesIO()
                    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
                        for report_name, report_bytes in generated_reports.items():
                            zip_file.writestr(report_name, report_bytes)
                    zip_buffer.seek(0)

                    st.download_button(
                        label="Download All Revenue Files (ZIP)",
                        data=zip_buffer.getvalue(),
                        file_name=f"adaptive_revenue_demand_outputs_{month_label}.zip",
                        mime="application/zip",
                    )

            except Exception as e:
                st.error(f"Error while processing revenue demand files: {e}")
    else:
        st.info("Please upload all three required revenue demand files.")
