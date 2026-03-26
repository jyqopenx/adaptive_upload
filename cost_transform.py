import re

import pandas as pd


def get_vendor_info(row, vendor_map_series_local):
    source_text = row.get("JOURNAL_LINE_DESCRIPTION", "")

    if pd.isna(source_text):
        source_text = ""
    source_text = str(source_text).strip()

    if source_text[:3] == "EE_":
        star_idx = source_text.find("*")
        first_gt_idx = source_text.find(">")

        start_idx = star_idx if star_idx != -1 else first_gt_idx

        if start_idx != -1:
            next_gt_idx = source_text.find(">", start_idx + 1)
            if next_gt_idx != -1:
                return source_text[start_idx + 1 : next_gt_idx].strip()

        return "General Spend"

    if len(source_text) >= 5 and source_text[4] == "*":
        code_to_lookup = source_text[:4].strip()

        vendor_name = vendor_map_series_local.get(code_to_lookup)
        if pd.notna(vendor_name):
            return vendor_name

        try:
            numeric_code = int(float(code_to_lookup))

            vendor_name = vendor_map_series_local.get(str(numeric_code))
            if pd.notna(vendor_name):
                return vendor_name

            vendor_name = vendor_map_series_local.get(numeric_code)
            if pd.notna(vendor_name):
                return vendor_name
        except (ValueError, TypeError):
            pass

        return "ADD VENDOR CODES"

    return "General Spend"


def normalize_vendor_mapping(vendor_mapping):
    vendor_mapping = vendor_mapping.copy()

    vendor_mapping["Code"] = (
        vendor_mapping["Code"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )

    vendor_mapping["Vendor Name"] = vendor_mapping["Vendor Name"].astype(str).str.strip()

    vendor_mapping = vendor_mapping.dropna(subset=["Code"])
    vendor_mapping = vendor_mapping[vendor_mapping["Code"] != ""]
    vendor_mapping = vendor_mapping.drop_duplicates(subset="Code", keep="first").reset_index(
        drop=True
    )

    return vendor_mapping


def add_missing_vendor_mappings(jedi_report_cleaned, vendor_mapping):
    vendor_map_series = vendor_mapping.set_index("Code")["Vendor Name"]

    mask_add_vendor = (
        jedi_report_cleaned["PARTY_NAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("ADD VENDOR CODES")
    )

    add_vendor_codes_rows = jedi_report_cleaned.loc[mask_add_vendor].copy()

    new_vendor_mappings_from_journal = []
    existing_codes = set(vendor_map_series.index.astype(str).str.strip())
    seen_new_codes = set()

    pattern = re.compile(r"^\s*([^*]+?)\*([^_]+?)(?:_.*)?\s*$")

    for _, row in add_vendor_codes_rows.iterrows():
        journal_desc = row.get("JOURNAL_LINE_DESCRIPTION", "")
        if pd.isna(journal_desc):
            continue

        journal_desc = str(journal_desc).strip()
        if not journal_desc:
            continue

        match = pattern.match(journal_desc)
        if not match:
            continue

        new_code = match.group(1).strip()
        new_vendor_name = match.group(2).strip()

        if not new_code or not new_vendor_name:
            continue

        if new_code not in existing_codes and new_code not in seen_new_codes:
            new_vendor_mappings_from_journal.append(
                {
                    "Code": new_code,
                    "Vendor Name": new_vendor_name,
                }
            )
            seen_new_codes.add(new_code)

    new_mappings_df = pd.DataFrame(new_vendor_mappings_from_journal)

    if not new_mappings_df.empty:
        new_mappings_df["Code"] = new_mappings_df["Code"].astype(str).str.strip()
        new_mappings_df["Vendor Name"] = new_mappings_df["Vendor Name"].astype(str).str.strip()

        vendor_mapping = pd.concat([vendor_mapping, new_mappings_df], ignore_index=True)
        vendor_mapping = vendor_mapping.drop_duplicates(subset="Code", keep="first").reset_index(
            drop=True
        )

    return vendor_mapping, new_mappings_df


def process_cost_files(raw_jedi_file, vendor_mapping_file, accounts_file):
    jedi_report = pd.read_excel(raw_jedi_file, header=1)
    vendor_mapping = pd.read_csv(vendor_mapping_file, encoding="latin1")
    accounts = pd.read_csv(accounts_file, encoding="latin1")

    jedi_report_cleaned = jedi_report.dropna(subset=["NATURAL_ACCOUNT", "USD_AMOUNT"]).copy()
    jedi_report_cleaned = jedi_report_cleaned[jedi_report_cleaned["COMPANY"] != 909].copy()

    if "PARTY_NAME" in jedi_report_cleaned.columns:
        jedi_report_cleaned = jedi_report_cleaned.sort_values(by="PARTY_NAME", ascending=True)

    vendor_mapping = normalize_vendor_mapping(vendor_mapping)
    vendor_map_series = vendor_mapping.set_index("Code")["Vendor Name"]

    blank_party_mask = (
        jedi_report_cleaned["PARTY_NAME"].isna()
        | (jedi_report_cleaned["PARTY_NAME"].astype(str).str.strip() == "")
    )

    jedi_report_cleaned.loc[blank_party_mask, "PARTY_NAME"] = jedi_report_cleaned.loc[
        blank_party_mask
    ].apply(lambda row: get_vendor_info(row, vendor_map_series), axis=1)

    vendor_mapping, new_mappings_df = add_missing_vendor_mappings(
        jedi_report_cleaned,
        vendor_mapping,
    )

    vendor_map_series = vendor_mapping.set_index("Code")["Vendor Name"]

    mask_add_vendor = (
        jedi_report_cleaned["PARTY_NAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("ADD VENDOR CODES")
    )

    jedi_report_cleaned.loc[mask_add_vendor, "PARTY_NAME"] = jedi_report_cleaned.loc[
        mask_add_vendor
    ].apply(lambda row: get_vendor_info(row, vendor_map_series), axis=1)

    remaining_add_vendor_codes = jedi_report_cleaned[
        jedi_report_cleaned["PARTY_NAME"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("ADD VENDOR CODES")
    ].copy()

    accounts.columns = [c.strip().upper() for c in accounts.columns]
    jedi_report_cleaned.columns = [c.strip().upper() for c in jedi_report_cleaned.columns]

    detail = jedi_report_cleaned.merge(
        accounts[[accounts.columns[0], accounts.columns[2]]].rename(
            columns={
                accounts.columns[0]: "NATURAL_ACCOUNT",
                accounts.columns[2]: "ACCOUNT_TYPE",
            }
        ),
        on="NATURAL_ACCOUNT",
        how="left",
    )

    detail["VENDOR_NAME_HELPER"] = detail["PARTY_NAME"].fillna("").astype(str).str.strip()
    detail["VENDOR_NAME_HELPER"] = detail["VENDOR_NAME_HELPER"].replace("", "(blank)")

    detail_cube = detail[detail["ACCOUNT_TYPE"].astype(str).str.strip() == "Cube"].copy()

    cos_operating_expenses_df = pd.DataFrame(
        {
            "Account": detail_cube["NATURAL_ACCOUNT"],
            "Level": detail_cube["COST_CENTER"],
            "Vendor Name": detail_cube["VENDOR_NAME_HELPER"],
            "Region": detail_cube["LOCATION"],
            "Revenue": detail_cube["USD_AMOUNT"],
        }
    )

    accounts_to_filter = [51115, 62290]
    filtered_accounts_df = jedi_report_cleaned[
        jedi_report_cleaned["NATURAL_ACCOUNT"].isin(accounts_to_filter)
    ].copy()

    filtered_accounts_df["Cleaned_Vendor_Name"] = filtered_accounts_df["PARTY_NAME"].apply(
        lambda x: "(blank)" if pd.isna(x) or str(x).strip() == "" else x
    )

    new_cos_operating_rows = filtered_accounts_df[
        ["NATURAL_ACCOUNT", "COST_CENTER", "Cleaned_Vendor_Name", "LOCATION", "USD_AMOUNT"]
    ].copy()

    new_cos_operating_rows = new_cos_operating_rows.rename(
        columns={
            "NATURAL_ACCOUNT": "Account",
            "COST_CENTER": "Level",
            "Cleaned_Vendor_Name": "Vendor Name",
            "LOCATION": "Region",
            "USD_AMOUNT": "Revenue",
        }
    )

    cos_operating_expenses_df = pd.concat(
        [cos_operating_expenses_df, new_cos_operating_rows],
        ignore_index=True,
    )

    return {
        "output": cos_operating_expenses_df,
        "remaining_add_vendor_codes": remaining_add_vendor_codes,
        "new_mappings_df": new_mappings_df,
        "vendor_mapping": vendor_mapping,
        "jedi_report_cleaned": jedi_report_cleaned,
    }
