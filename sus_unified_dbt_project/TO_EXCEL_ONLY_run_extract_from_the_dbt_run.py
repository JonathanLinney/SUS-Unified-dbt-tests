import snowflake.connector
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from datetime import datetime
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def query_snowflake_activity(sql):
    """Connects to Snowflake and returns a DataFrame of the activity."""
    conn = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
    )
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        df["ACTIVITY_DATE"] = pd.to_datetime(df["ACTIVITY_DATE"])
        df["DAY_LABEL"] = df["ACTIVITY_DATE"].dt.strftime("%d/%m/%Y")
        return df
    finally:
        conn.close()

def build_summary_table(ws, df, title, start_row):
    """Builds the formatted summary table at the top of the Excel sheet."""
    ws.cell(row=start_row, column=1, value=title).alignment = Alignment(horizontal="left")
    headers = ["Provider", "APC Missing", "OP Missing", "ECDS Missing", "Total Missing", "Action Required"]
    ws.append(headers)

    for _, row in df.iterrows():
        ws.append([
            row["PROVIDER"], 
            row["APC_MISSING_DAYS"], 
            row["OP_MISSING_DAYS"],
            row["ECDS_MISSING_DAYS"],
            row["TOTAL_MISSING_SUBMISSIONS"], 
            row["ACTION_REQUIRED"]
        ])

    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    thin_border = Border(left=Side(style="thin"), right=Side(style="thin"),
                         top=Side(style="thin"), bottom=Side(style="thin"))

    header_row = start_row + 1
    first_data_row = header_row + 1
    last_row = ws.max_row

    for cell in ws.iter_rows(min_row=header_row, max_row=header_row, min_col=1, max_col=6):
        for c in cell:
            c.border = thin_border

    for row in ws.iter_rows(min_row=first_data_row, max_row=last_row, min_col=1, max_col=6):
        total = row[4]
        for c in row:
            c.border = thin_border
        if int(total.value) > 0:
            for c in row: c.fill = red_fill
        else:
            for c in row: c.fill = green_fill

def build_pivot_table(ws, df, title, start_row):
    """Builds the detailed daily pivot table with anomaly detection."""
    if df.empty:
        return
        
    df["ACTIVITY_DATE"] = pd.to_datetime(df["ACTIVITY_DATE"])
    day_order = df[["ACTIVITY_DATE", "DAY_LABEL"]].drop_duplicates().sort_values("ACTIVITY_DATE")
    day_labels_sorted = day_order["DAY_LABEL"].tolist()
    weekday_labels_sorted = day_order["ACTIVITY_DATE"].dt.day_name().str[:3].tolist()
    providers = sorted(df["PROVIDER"].unique())

    pivot_records = df.pivot(index="PROVIDER", columns="DAY_LABEL", values="RECORDS")
    pivot_records = pivot_records.reindex(index=providers, columns=day_labels_sorted)

    provider_stats = {}
    for provider in providers:
        p_data = df[df["PROVIDER"] == provider]
        wkday = p_data[p_data["ACTIVITY_DATE"].dt.weekday < 5]["RECORDS"]
        wkend = p_data[p_data["ACTIVITY_DATE"].dt.weekday >= 5]["RECORDS"]
        provider_stats[provider] = {
            "weekday": {"mean": wkday.mean(), "std": wkday.std()} if len(wkday[wkday > 0]) > 2 else None,
            "weekend": {"mean": wkend.mean(), "std": wkend.std()} if len(wkend[wkend > 0]) > 2 else None
        }

    timestamp = datetime.now().strftime("Report generated at %H:%M GMT, %d %b %Y")
    ws.cell(row=start_row, column=1, value=timestamp).alignment = Alignment(horizontal="left")
    ws.cell(row=start_row+2, column=1, value=title)

    header_row = start_row+3
    ws.append([""] + weekday_labels_sorted)
    ws.append(["Provider"] + day_labels_sorted)

    for provider, row in pivot_records.iterrows():
        mapped_row = ["MISSING" if (pd.isna(v) or v == 0) else int(v) for v in row.values]
        ws.append([provider] + mapped_row)

    # Styling
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB9C", end_color="FFEB9C", fill_type="solid")
    orange_fill = PatternFill(start_color="FFC000", end_color="FFC000", fill_type="solid")
    thin_border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))

    first_data_row = header_row+2
    last_row = ws.max_row
    last_col = len(day_labels_sorted) + 1

    for row_idx, row in enumerate(ws.iter_rows(min_row=first_data_row, max_row=last_row, min_col=1, max_col=last_col)):
        p_name = providers[row_idx]
        for j, c in enumerate(row, start=1):
            c.border = thin_border
            if j == 1: continue
            if c.value == "MISSING":
                c.fill = red_fill
            elif isinstance(c.value, (int, float)):
                act_date = day_order.iloc[j-2]["ACTIVITY_DATE"]
                stats = provider_stats[p_name]["weekend" if act_date.weekday() >= 5 else "weekday"]
                if stats and stats["std"] and stats["std"] > 0:
                    z_score = abs((float(c.value) - stats["mean"]) / stats["std"])
                    if z_score > 3: c.fill = orange_fill
                    elif z_score > 2: c.fill = yellow_fill
                    else: c.fill = green_fill
                else: c.fill = green_fill

    ws.column_dimensions["A"].width = 35
    for i in range(2, last_col+1):
        ws.column_dimensions[get_column_letter(i)].width = 11.36

def export_to_excel(df_summary, df_inpatient, df_op, df_ecds, filename="provider_status.xlsx"):
    wb = Workbook()
    ws = wb.active
    ws.title = "Provider Daily Status"

    build_summary_table(ws, df_summary, "Provider Missing Days Summary (Rolling 30 Day Monitoring Window)", start_row=1)
    ws.append([]); ws.append([])
    
    build_pivot_table(ws, df_inpatient, "Inpatient Provider Daily Status", start_row=ws.max_row+1)
    build_pivot_table(ws, df_op, "Outpatient Provider Daily Status", start_row=ws.max_row+3)
    build_pivot_table(ws, df_ecds, "Emergency Attendances (ECDS) Daily Status", start_row=ws.max_row+3)

    ws.freeze_panes = ws["B2"]
    wb.save(filename)
    print(f"Excel report saved as {filename}")
    return filename

if __name__ == "__main__":
    print("--- Starting Extraction ---")
    
    # Setup Window
    now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = now - pd.Timedelta(days=44)
    end_date = now - pd.Timedelta(days=15)
    expected_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    num_expected_days = len(expected_dates)

    # 1. Fetch Detail Data
    query_template = "SELECT PROVIDER, ACTIVITY_DATE, RECORDS FROM {table} WHERE ACTIVITY_DATE >= CURRENT_DATE - INTERVAL '44 days' AND ACTIVITY_DATE < CURRENT_DATE - INTERVAL '14 days'"
    
    df_apc = query_snowflake_activity(query_template.format(table="PROVIDER_DAILY_APC_ACTIVITY_DBT"))
    df_op = query_snowflake_activity(query_template.format(table="PROVIDER_DAILY_OP_ACTIVITY_DBT"))
    df_ecds = query_snowflake_activity(query_template.format(table="PROVIDER_DAILY_ECDS_ACTIVITY_DBT"))

    # 2. Establish Historical Scope
    def get_scope(table_name):
        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            role=os.environ.get("SNOWFLAKE_ROLE"),
            authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        )
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT PROVIDER FROM {table_name}")
        providers = [row[0] for row in cur.fetchall()]
        conn.close()
        return providers

    print("Checking Historical Scope...")
    scope_apc = get_scope("PROVIDER_DAILY_APC_ACTIVITY_DBT")
    scope_op = get_scope("PROVIDER_DAILY_OP_ACTIVITY_DBT")
    scope_ecds = get_scope("PROVIDER_DAILY_ECDS_ACTIVITY_DBT")

    # 3. Build Synchronized Summary
    all_providers = sorted(set(scope_apc) | set(scope_op) | set(scope_ecds))
    summary_list = []

    for p in all_providers:
        def calc_gaps(df, provider, scope_list):
            if provider not in scope_list: return 0
            present = df[(df["PROVIDER"] == provider) & (df["RECORDS"] > 0)]["ACTIVITY_DATE"].nunique()
            return max(0, num_expected_days - present)

        m_apc = calc_gaps(df_apc, p, scope_apc)
        m_op = calc_gaps(df_op, p, scope_op)
        m_ecds = calc_gaps(df_ecds, p, scope_ecds)
        total = m_apc + m_op + m_ecds
        
        summary_list.append({
            "PROVIDER": p, "APC_MISSING_DAYS": m_apc, "OP_MISSING_DAYS": m_op,
            "ECDS_MISSING_DAYS": m_ecds, "TOTAL_MISSING_SUBMISSIONS": total,
            "ACTION_REQUIRED": "Contact ISL" if total > 0 else "None"
        })

    df_summary = pd.DataFrame(summary_list)

    # 4. Final Export
    fn = export_to_excel(df_summary, df_apc, df_op, df_ecds)
    
    try:
        os.startfile(fn)
    except:
        pass
        
    print("Process Complete!")