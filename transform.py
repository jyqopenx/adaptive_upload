import re
from io import BytesIO

import pandas as pd


# =========================
# Adaptive Cost Upload
# =========================
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


# =========================
# Adaptive Revenue Demand Upload
# =========================
def _safe_read_instruction_sheet(instructions_file, sheet_index):
    instructions_file.seek(0)
    return pd.read_excel(instructions_file, sheet_name=sheet_index, header=None)


def _normalize_integer_series(series):
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def _derive_month_fields(demand):
    if "utc_month_sid" not in demand.columns:
        raise ValueError("Column 'utc_month_sid' is required in demand data.")

    month_values = pd.to_numeric(demand["utc_month_sid"], errors="coerce").dropna()
    if month_values.empty:
        raise ValueError("No valid values found in 'utc_month_sid'.")

    latest_raw = int(month_values.max())
    latest_raw_str = str(latest_raw)

    parsed_month = pd.to_datetime(latest_raw_str, format="%Y%m", errors="coerce")
    if pd.isna(parsed_month):
        parsed_month = pd.to_datetime(latest_raw_str, format="%Y%m%d", errors="coerce")

    if pd.isna(parsed_month):
        raise ValueError(
            "Could not parse 'utc_month_sid'. Expected values like YYYYMM or YYYYMMDD."
        )

    month_column_name = parsed_month.strftime("%b-%y")
    latest_month_str = parsed_month.strftime("%b %Y")
    return month_column_name, latest_month_str


def _apply_excel_formatting(writer, instructions_df, data_df, output_sheet_name):
    instructions_df.to_excel(writer, sheet_name="Instructions", index=False, header=False)
    data_df.to_excel(writer, sheet_name=output_sheet_name, index=False)

    workbook = writer.book

    instructions_ws = writer.sheets["Instructions"]
    data_ws = writer.sheets[output_sheet_name]

    bold_format = workbook.add_format({"bold": True})
    instructions_ws.set_row(0, None, bold_format)

    bold_no_border_format = workbook.add_format(
        {
            "bold": True,
            "bottom": 0,
            "top": 0,
            "left": 0,
            "right": 0,
        }
    )
    data_ws.set_row(0, None, bold_no_border_format)


def _build_revenue_report_file(instructions_df, final_df, output_sheet_name):
    output_buffer = BytesIO()
    with pd.ExcelWriter(output_buffer, engine="xlsxwriter") as writer:
        _apply_excel_formatting(writer, instructions_df, final_df, output_sheet_name)
    output_buffer.seek(0)
    return output_buffer.getvalue()


def _standardize_demand_id_file(demand_id_df):
    demand_id_df = demand_id_df.copy()
    demand_id_df.columns = [str(c).strip() for c in demand_id_df.columns]

    if "dsp_id" in demand_id_df.columns:
        id_col = "dsp_id"
    else:
        id_col = demand_id_df.columns[0]

    demand_id_df = demand_id_df[[id_col]].rename(columns={id_col: "dsp_id"})
    demand_id_df["dsp_id"] = _normalize_integer_series(demand_id_df["dsp_id"])
    demand_id_df = demand_id_df.dropna(subset=["dsp_id"]).drop_duplicates().reset_index(drop=True)

    return demand_id_df


def generate_revenue_reports_iteration(
    demand,
    instructions_df,
    device_type,
    environment,
    report_identifier,
    device_prefix,
    month_column_name,
    latest_month_str,
):
    filtered_demand_iter = demand[
        (demand["device_type"] == device_type) & (demand["environment"] == environment)
    ].copy()

    if filtered_demand_iter.empty:
        return None

    required_columns = [
        "Level",
        "AdvertiserAccountID",
        "integration",
        "ad_format",
        "video_format",
        "transaction_type",
        "bidout_partner",
        "tot_mkt_impressions",
        "tot_spend_usd",
    ]
    missing_cols = [c for c in required_columns if c not in filtered_demand_iter.columns]
    if missing_cols:
        raise ValueError(
            f"Missing required columns in demand data for revenue report: {missing_cols}"
        )

    filtered_demand_iter["ad_form_2"] = filtered_demand_iter.apply(
        lambda row: "display" if row["ad_format"] == "BANNER" else row["video_format"],
        axis=1,
    )

    grouped_demand_iter = (
        filtered_demand_iter.groupby(
            [
                "Level",
                "AdvertiserAccountID",
                "integration",
                "ad_form_2",
                "transaction_type",
                "bidout_partner",
            ],
            dropna=False,
        )
        .agg(
            tot_mkt_impressions_sum=("tot_mkt_impressions", "sum"),
            tot_spend_usd_sum=("tot_spend_usd", "sum"),
        )
        .reset_index()
    )

    melted_demand_iter = grouped_demand_iter.melt(
        id_vars=[
            "Level",
            "AdvertiserAccountID",
            "integration",
            "ad_form_2",
            "transaction_type",
            "bidout_partner",
        ],
        var_name="Account",
        value_name=month_column_name,
    )

    melted_demand_iter["Account"] = melted_demand_iter["Account"].map(
        {
            "tot_mkt_impressions_sum": "Sum of tot_mkt_impressions",
            "tot_spend_usd_sum": "Sum of tot_spend_usd",
        }
    )

    final_report_iter = melted_demand_iter.rename(
        columns={
            "Level": "Level Code",
            "AdvertiserAccountID": "Demand Partner ID Code",
            "integration": "Integration Code",
            "ad_form_2": "Ad_Format Code",
            "transaction_type": "Transaction_Type Code",
            "bidout_partner": "Bidout_Partner Code",
        }
    )

    output_columns_order = [
        "Account",
        "Level Code",
        "Demand Partner ID Code",
        "Integration Code",
        "Ad_Format Code",
        "Transaction_Type Code",
        "Bidout_Partner Code",
        month_column_name,
    ]
    final_report_iter = final_report_iter[output_columns_order]

    triggers_df_iter = filtered_demand_iter[
        [
            "Level",
            "AdvertiserAccountID",
            "integration",
            "ad_form_2",
            "transaction_type",
            "bidout_partner",
        ]
    ].drop_duplicates().reset_index(drop=True)

    triggers_df_iter["Account"] = "triggers"
    triggers_df_iter[month_column_name] = 1

    final_triggers_report_iter = triggers_df_iter.rename(
        columns={
            "Level": "Level Code",
            "AdvertiserAccountID": "Demand Partner ID Code",
            "integration": "Integration Code",
            "ad_form_2": "Ad_Format Code",
            "transaction_type": "Transaction_Type Code",
            "bidout_partner": "Bidout_Partner Code",
        }
    )

    final_triggers_report_iter = final_triggers_report_iter[output_columns_order]

    main_sheet_name = f"_{report_identifier.replace('_', '.')} Rev - Demand - Model - {device_prefix}"
    trigger_sheet_name = main_sheet_name

    report_filename = (
        f"_{report_identifier}_Rev_-_Demand_-_Model_-__{device_prefix} LOAD FILE "
        f"({latest_month_str}).xlsx"
    )
    trigger_filename = (
        f"_{report_identifier}_Rev_-_Demand_-_Model_-__{device_prefix} LOAD FILE "
        f"({latest_month_str}) - trigger.xlsx"
    )

    report_bytes = _build_revenue_report_file(
        instructions_df=instructions_df,
        final_df=final_report_iter,
        output_sheet_name=main_sheet_name,
    )

    trigger_bytes = _build_revenue_report_file(
        instructions_df=instructions_df,
        final_df=final_triggers_report_iter,
        output_sheet_name=trigger_sheet_name,
    )

    return {
        "report_filename": report_filename,
        "report_bytes": report_bytes,
        "trigger_filename": trigger_filename,
        "trigger_bytes": trigger_bytes,
    }


def process_revenue_files(instructions_file, demand_data_file, demand_id_file):
    instructions_sheet1 = _safe_read_instruction_sheet(instructions_file, 0)
    instructions_sheet2 = _safe_read_instruction_sheet(instructions_file, 1)
    instructions_sheet3 = _safe_read_instruction_sheet(instructions_file, 2)

    demand_data_file.seek(0)
    demand = pd.read_csv(demand_data_file, encoding="latin1")

    demand_id_file.seek(0)
    demand_id_raw = pd.read_csv(demand_id_file, encoding="latin1")

    if "AdvertiserAccountID" not in demand.columns:
        raise ValueError("Column 'AdvertiserAccountID' is required in demand data.")
    if "advertiser_account_name" not in demand.columns:
        raise ValueError("Column 'advertiser_account_name' is required in demand data.")

    known_demand_ids_df = _standardize_demand_id_file(demand_id_raw)

    demand = demand.copy()
    demand["AdvertiserAccountID"] = _normalize_integer_series(demand["AdvertiserAccountID"])

    demand_advertiser_ids = pd.Series(demand["AdvertiserAccountID"].dropna().unique())
    known_dsp_ids = pd.Series(known_demand_ids_df["dsp_id"].dropna().unique())

    new_id_list = [
        adv_id for adv_id in demand_advertiser_ids.tolist() if adv_id not in known_dsp_ids.tolist()
    ]

    new_mappings_df = (
        demand[demand["AdvertiserAccountID"].isin(new_id_list)][
            ["AdvertiserAccountID", "advertiser_account_name"]
        ]
        .drop_duplicates()
        .rename(
            columns={
                "AdvertiserAccountID": "dsp_id",
                "advertiser_account_name": "dsp_name",
            }
        )
        .sort_values(by=["dsp_id", "dsp_name"], na_position="last")
        .reset_index(drop=True)
    )

    if not new_mappings_df.empty:
        new_mappings_df["dsp_id"] = new_mappings_df["dsp_id"].astype("Int64")

    demand = demand.drop(columns=["advertiser_account_name"])

    month_column_name, latest_month_str = _derive_month_fields(demand)

    iterations = [
        {
            "device_type": "desktop",
            "environment": "web",
            "report_identifier": "B_01",
            "device_prefix": "De",
            "instructions_df": instructions_sheet1,
        },
        {
            "device_type": "mobile",
            "environment": "web",
            "report_identifier": "B_02",
            "device_prefix": "Mo",
            "instructions_df": instructions_sheet2,
        },
        {
            "device_type": "mobile",
            "environment": "app",
            "report_identifier": "B_03",
            "device_prefix": "Mo",
            "instructions_df": instructions_sheet3,
        },
    ]

    generated_reports = {}

    for config in iterations:
        report_result = generate_revenue_reports_iteration(
            demand=demand,
            instructions_df=config["instructions_df"],
            device_type=config["device_type"],
            environment=config["environment"],
            report_identifier=config["report_identifier"],
            device_prefix=config["device_prefix"],
            month_column_name=month_column_name,
            latest_month_str=latest_month_str,
        )

        if report_result is None:
            continue

        generated_reports[report_result["report_filename"]] = report_result["report_bytes"]
        generated_reports[report_result["trigger_filename"]] = report_result["trigger_bytes"]

    return {
        "known_demand_ids_df": known_demand_ids_df,
        "new_mappings_df": new_mappings_df,
        "generated_reports": generated_reports,
        "month_label": latest_month_str,
    }
