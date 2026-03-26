from io import BytesIO

import pandas as pd


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

    month_start_date = parsed_month.replace(day=1)
    month_file_label = parsed_month.strftime("%Y%m")
    return month_start_date, month_file_label


def _apply_excel_formatting(writer, instructions_df, data_df, output_sheet_name):
    instructions_df.to_excel(writer, sheet_name="Instructions", index=False, header=False)
    data_df.to_excel(writer, sheet_name=output_sheet_name, index=False)

    workbook = writer.book

    instructions_ws = writer.sheets["Instructions"]
    data_ws = writer.sheets[output_sheet_name]

    bold_format = workbook.add_format({"bold": True})
    date_header_format = workbook.add_format(
        {
            "bold": True,
            "num_format": "m/d/yyyy",
            "bottom": 0,
            "top": 0,
            "left": 0,
            "right": 0,
        }
    )
    bold_no_border_format = workbook.add_format(
        {
            "bold": True,
            "bottom": 0,
            "top": 0,
            "left": 0,
            "right": 0,
        }
    )

    instructions_ws.set_row(0, None, bold_format)

    for col_idx, col_name in enumerate(data_df.columns):
        if isinstance(col_name, pd.Timestamp):
            data_ws.write_datetime(0, col_idx, col_name.to_pydatetime(), date_header_format)
        else:
            data_ws.write(0, col_idx, col_name, bold_no_border_format)


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
    month_file_label,
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
        f"_{report_identifier}_Rev_-_Demand_-_Model_-_{device_prefix} LOAD FILE "
        f"({month_file_label}).xlsx"
    )
    trigger_filename = (
        f"_{report_identifier}_Rev_-_Demand_-_Model_-_{device_prefix} LOAD FILE "
        f"({month_file_label})- trigger.xlsx"
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

    month_column_name, month_file_label = _derive_month_fields(demand)

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
            month_file_label=month_file_label,
        )

        if report_result is None:
            continue

        generated_reports[report_result["report_filename"]] = report_result["report_bytes"]
        generated_reports[report_result["trigger_filename"]] = report_result["trigger_bytes"]

    return {
        "known_demand_ids_df": known_demand_ids_df,
        "new_mappings_df": new_mappings_df,
        "generated_reports": generated_reports,
        "month_label": month_file_label,
    }
