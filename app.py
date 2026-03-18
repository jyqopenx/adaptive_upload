import streamlit as st
import pandas as pd
from io import BytesIO

from transform import process_files

st.set_page_config(page_title="Adaptive Cost Upload Tool", layout="wide")

st.title("Adaptive Cost Upload Tool")
st.write("Upload the required files and generate the final COS & Operating Expenses output.")

with st.sidebar:
    st.header("Required files")
    st.markdown(
        """
        Upload:
        1. Raw JEDI report (.xlsx)
        2. Vendor mapping (.csv)
        3. Accounts (.csv)
        """
    )

raw_jedi_file = st.file_uploader("Upload raw JEDI report", type=["xlsx"])
vendor_mapping_file = st.file_uploader("Upload vendor mapping CSV", type=["csv"])
accounts_file = st.file_uploader("Upload Accounts CSV", type=["csv"])

if raw_jedi_file and vendor_mapping_file and accounts_file:
    if st.button("Generate output"):
        try:
            with st.spinner("Processing files..."):
                result = process_files(
                    raw_jedi_file=raw_jedi_file,
                    vendor_mapping_file=vendor_mapping_file,
                    accounts_file=accounts_file,
                )

            cos_operating_expenses_df = result["output"]
            remaining_add_vendor_codes = result["remaining_add_vendor_codes"]
            new_mappings_df = result["new_mappings_df"]
            updated_vendor_mapping_df = result["vendor_mapping"]

            st.success("Processing complete.")

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
                st.dataframe(
                    remaining_add_vendor_codes[
                        ["PARTY_NAME", "JOURNAL_LINE_DESCRIPTION", "USD_AMOUNT"]
                    ].head(50),
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
                label="Download Output Excel File",
                data=output_buffer,
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

            if not new_mappings_df.empty:
                new_mappings_csv = new_mappings_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    label="Download Only New Vendor Mappings (CSV)",
                    data=new_mappings_csv,
                    file_name="new_vendor_mappings.csv",
                    mime="text/csv",
                )

        except Exception as e:
            st.error(f"Error while processing files: {e}")
else:
    st.info("Please upload all three required files.")
