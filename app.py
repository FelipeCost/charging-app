import streamlit as st
import pandas as pd
from datetime import datetime, time
from pathlib import Path
import os
from s3_utils import read_csv_s3, write_csv_s3
import io
from botocore.exceptions import ClientError
import boto3



LOG_FILE = "charging_log.csv"
HOUSE_PRICE_FILE = "house_prices.csv"
PUBLIC_PRICE_FILE = "public_prices.csv"
CONFIG_FILE = "config.csv"
SESSION_FILE = "open_session.csv"
S3_BUCKET = os.environ.get("S3_BUCKET")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-2")

st.set_page_config(page_title="Charging Log", layout="centered")

full_range = 246 #st.number_input("Estimated full range at 100% (miles)", min_value=1)

def fetch_csv_from_s3(key):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return obj["Body"].read()

params = st.query_params

if "last_home_cost" not in st.session_state:
    st.session_state.last_home_cost = None

# ---------- Utils ----------

def load_or_create(file, cols):
    df = read_csv_s3(file, cols)
    if len(df) == 0:
        write_csv_s3(df, file)
    return df

def load_config():
    df = read_csv_s3(CONFIG_FILE, ["BatteryCapacity_kWh"])
    if len(df) == 0:
        df = pd.DataFrame([{"BatteryCapacity_kWh": 0}])
        write_csv_s3(df, CONFIG_FILE)
    return df



def parse_time(t):
    if pd.isna(t):
        return time(0, 0)

    if isinstance(t, (float, int)) and not pd.isna(t):
        hours = int(t)
        minutes = int((t - hours) * 60)
        return time(hours, minutes)

    if isinstance(t, str):
        try:
            return datetime.strptime(t, "%H:%M:%S").time()
        except:
            return datetime.strptime(t, "%H:%M").time()

    return time(0, 0)



def get_weighted_price(row, start_dt, end_dt):
    start = parse_time(row["Start Time"])
    end = parse_time(row["End Time"])

    price_a = float(row["Price A"])
    price_b = float(row["Price B"])
    add_p = float(row["Additional Price"])

    total_cost = 0
    total_hours = 0
    current = start_dt

    while current < end_dt:
        nxt = min(current + pd.Timedelta(minutes=1), end_dt)
        cur_time = current.time()

        if start <= end:
            in_range = start <= cur_time <= end
        else:
            in_range = cur_time >= start or cur_time <= end

        price = price_a if in_range else price_b
        total_cost += price * ((nxt - current).total_seconds() / 3600)
        total_hours += (nxt - current).total_seconds() / 3600
        current = nxt

    return round((total_cost / total_hours) + add_p, 4)

def load_session():
    df = read_csv_s3(SESSION_FILE)
    if len(df) > 0:
        return df.iloc[0]
    return None

def save_session(data):
    write_csv_s3(pd.DataFrame([data]), SESSION_FILE)

def clear_session():
    write_csv_s3(pd.DataFrame(columns=[
        "Timestamp Start",
        "Location",
        "Company",
        "Battery Start %"
    ]), SESSION_FILE)





# ---------- Load tables ----------

house_prices = load_or_create(
    HOUSE_PRICE_FILE,
    ["Start Time", "End Time", "Price A", "Price B", "Additional Price"]
)

# @st.cache_data
def load_public_prices():
    return load_or_create(
        PUBLIC_PRICE_FILE,
        ["Company", "Start Time", "End Time", "Price A", "Price B", "Additional Price"]
    )

public_prices = load_public_prices()


LOG_COLUMNS = [
    "Timestamp Start",
    "Timestamp End",
    "Duration Hours",
    "Location",
    "Company",
    "Battery Start %",
    "Battery End %",
    "Range Start",    # NEW
    "Range End",      # NEW
    "kWh",
    "Price per kWh",
    "Total Cost"
]



log_df = load_or_create(LOG_FILE, LOG_COLUMNS)
# preencher NaN se colunas novas não existirem
for col in ["Range Start", "Range End"]:
    if col not in log_df.columns:
        log_df[col] = None


config = load_config()
battery_capacity = float(config.iloc[0]["BatteryCapacity_kWh"])


# ---------- UI ----------

st.title("🔌 Charging Log")
tab_log, tab_history, tab_admin = st.tabs([
    "📝 Log Charging",
    "📊 History",
    "⚙️ Configure Prices"
])




with tab_log:

    if st.session_state.last_home_cost is not None:
        st.success(f"🏠 Last home charging cost: £{st.session_state.last_home_cost:.2f}")

    session = load_session()

    if session is None:
        use_now = st.checkbox("Use current date and time", value=True)

        if use_now:
            start_ts = datetime.now()
        else:
            d = st.date_input("Start Date")
            st.subheader("Start Time")
            c1, c2 = st.columns(2)
            with c1:
                start_hour = st.selectbox(
                    "Hour",
                    list(range(0, 24)),
                    format_func=lambda x: f"{x:02d}",
                    key="start_hour"
                )

            with c2:
                start_minute = st.selectbox(
                    "Minute",
                    list(range(0, 60)),
                    format_func=lambda x: f"{x:02d}",
                    key="start_min"
                )

            start_time = time(start_hour, start_minute)
            start_ts = datetime.combine(d, start_time)
            
    else:
        start_ts = pd.to_datetime(session["Timestamp Start"])


    if session is None:
        mode = "start"
        st.success("🟢 No open session — start a new charging session")
    else:
        mode = "end"
        st.warning(f"🟡 Open session since {session['Timestamp Start']}")

    if session is None:
        location = st.selectbox("Location", ["Home", "Public"])
    else:
        location = session["Location"]
        st.info(f"Location: {location}")


    kwh = 0
    bat_start = 0
    bat_end = 0

    company = ""
    battery = ""

    price_per_kwh = 0



    if mode == "start":

    # start_ts has already been defined above

        if location == "Home":
            company = ""

        if location == "Public":
            companies = sorted(public_prices["Company"].dropna().unique().tolist())
            companies = ["➕ New Company"] + companies
            selected = st.selectbox("Company", companies)

            if selected == "➕ New Company":
                company = st.text_input("New Company")
            else:
                company = selected

    range_start = st.number_input("Estimated range at start (miles)", min_value=0)

    if location == "Public" and company.strip() == "":
        st.error("Please enter the company.")
        st.stop()

    if st.button("Start Charging"):
        save_session({
                    "Timestamp Start": start_ts,
                    "Location": location,
                    "Company": company,
                    "Battery Start %": bat_start,
                    "Range Start": range_start   # NEW
                    })

        st.success("Session started!")
        st.rerun()

if mode == "end":

    company = session["Company"]
    st.info(f"Company: {company}")
    st.caption("Session in progress — company locked")
    range_end = st.number_input("Estimated range at end (miles)", min_value=0)
    st.subheader("End Time")
    c1, c2 = st.columns(2)
    with c1:
        end_hour = st.selectbox(
            "Hour",
            list(range(0, 24)),
            format_func=lambda x: f"{x:02d}",
            key="end_hour"
        )

    with c2:
        end_minute = st.selectbox(
            "Minute",
            list(range(0, 60)),
            format_func=lambda x: f"{x:02d}",
            key="end_min"
        )

    end_time = time(end_hour, end_minute)

    start_ts = pd.to_datetime(session["Timestamp Start"])
    end_ts = datetime.combine(start_ts.date(), end_time)
    if end_ts <= start_ts:
        end_ts += pd.Timedelta(days=1)  # assume charging passed midnight

    charge_speed = st.number_input("Estimated charging speed (kW - optional)", step=0.001)
 
    # Only on public charging, optional
    total_manual = None
    if session["Location"] == "Public":
        total_manual = st.number_input("Total Price (optional)", step=0.01)

    duration_hours = round((end_ts - start_ts).total_seconds() / 3600, 2)
    st.info(f"⏱ Duration: {duration_hours} h")

    # Auto Estimating
    battery_delta = bat_end - float(session["Battery Start %"])
    kwh_from_battery = round((battery_delta / 100) * battery_capacity, 2) if battery_delta > 0 else 0
    kwh_from_speed = round(charge_speed * duration_hours, 2) if charge_speed > 0 else 0

    # Smart suggestion
    suggested_kwh = kwh_from_speed if charge_speed > 0 else kwh_from_battery

    kwh_manual = st.number_input(
        "kWh (optional)",
        value=float(suggested_kwh) if suggested_kwh > 0 else 0.0,
        step=0.001
    )


    if st.button("Finish Charging"):

        # ---------- kWh calculation priority ----------

        if kwh_manual > 0:
            kwh = round(kwh_manual, 2)

        # Use range based calculation if available
        elif range_end > range_start and full_range > 0:
            range_delta = range_end - range_start
            perc_gained = range_delta / full_range
            kwh = round(perc_gained * battery_capacity, 2)

        # Fallback to battery %
        else:
            delta = bat_end - float(session["Battery Start %"])

            # if not (0 <= bat_end <= 100):
            #     st.error("Final battery must be between 0 and 100%.")
            #     st.stop()

            # if delta <= 0:
            #     st.error("Final battery must be greater than the initial.")
            #     st.stop()

            kwh = round((delta / 100) * battery_capacity, 2)



        if session["Location"] == "Home":
            if len(house_prices) == 0:
                st.error("Please configure the home price first.")
                st.stop()
            row = house_prices.iloc[0]

        else:
            match = public_prices[public_prices["Company"] == session["Company"]]

            if len(match) == 0:
                st.error("Company price not found.")
                st.stop()

            row = match.iloc[0]

        # If manual price entered, recalculate price per kWh
        if total_manual is not None and total_manual > 0:
            total = total_manual
            price = round(total / kwh, 4)
        else:
            price = get_weighted_price(row, start_ts, end_ts)
            total = round(price * kwh, 2)

        # Show charging cost, if charging at home
        if session["Location"] == "Home":
            st.session_state.last_home_cost = total
        else:
            st.session_state.last_home_cost = None


        new_row = pd.DataFrame([{
                                "Timestamp Start": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                                "Timestamp End": end_ts.strftime("%Y-%m-%d %H:%M:%S"),
                                "Duration Hours": duration_hours,
                                "Location": session["Location"],
                                "Company": session["Company"],
                                "Battery Start %": session["Battery Start %"],
                                "Battery End %": bat_end,
                                "Range Start": session.get("Range Start", None),  # NEW
                                "Range End": range_end,                           # NEW
                                "kWh": kwh,
                                "Price per kWh": price,
                                "Total Cost": total
                            }])



        log_df = pd.concat([log_df, new_row], ignore_index=True)[LOG_COLUMNS]
        write_csv_s3(log_df, LOG_FILE)

        clear_session()
        st.success("Charging finished!")
        st.rerun()

with tab_history:

    st.subheader("📊 Charging History (Editable)")

    history_df = read_csv_s3(LOG_FILE)

    if len(history_df) == 0:
        st.info("No charging sessions recorded yet.")
        st.stop()

    history_df["Timestamp Start"] = pd.to_datetime(history_df["Timestamp Start"], errors="coerce")
    history_df["Timestamp End"]   = pd.to_datetime(history_df["Timestamp End"], errors="coerce")

    history_df = history_df.sort_values("Timestamp Start", ascending=False)

    edited_df = st.data_editor(
        history_df,
        num_rows="dynamic",
        use_container_width=True
    )

    st.caption("Edit any field directly in the table above and click Save to persist changes.")

    if st.button("💾 Save changes"):
        edited_df["Timestamp Start"] = edited_df["Timestamp Start"].dt.strftime("%Y-%m-%d %H:%M:%S")
        edited_df["Timestamp End"]   = edited_df["Timestamp End"].dt.strftime("%Y-%m-%d %H:%M:%S")

        write_csv_s3(edited_df, LOG_FILE)
        st.success("History updated successfully.")
        st.rerun()


# ---------- Admin ----------

with tab_admin:

    with st.expander("🔧 Vehicle Parameters"):
        cap = st.number_input("Total battery capacity (kWh)", step=1.0, value=battery_capacity)

        if st.button("Save Parameters"):
            write_csv_s3(pd.DataFrame([{"BatteryCapacity_kWh": cap}]), CONFIG_FILE)
            st.success("Parameters saved!")
            st.rerun()

    with st.expander("⚙️ Set Prices"):

        st.subheader("🏠 Home Price")

        c1, c2 = st.columns(2)
        with c1:
            h_start = st.time_input("Start Time (Home)", value=time(0,0))
            h_end   = st.time_input("End Time (Home)", value=time(23,59))
        with c2:
            h_a = st.number_input("Price A (Home)", step=0.001)
            h_b = st.number_input("Price B (Home)", step=0.001)
            h_add = st.number_input("Additional Price (Home)", step=0.001)

        if st.button("Save Home Price"):
            house_prices = pd.DataFrame([{
                "Start Time": h_start.strftime("%H:%M:%S"),
                "End Time": h_end.strftime("%H:%M:%S"),
                "Price A": h_a,
                "Price B": h_b,
                "Additional Price": h_add
            }])
            write_csv_s3(house_prices, HOUSE_PRICE_FILE)
            st.success("Home price saved!")


        st.divider()
        st.subheader("🌍 Public Price")

        company = st.text_input("Company", key="price_company")

        c1, c2 = st.columns(2)
        with c1:
            p_start = st.time_input("Start Time (Public)", value=time(0,0), key="ps")
            p_end   = st.time_input("End Time (Public)", value=time(23,59), key="pe")
        with c2:
            p_a = st.number_input("Price A (Public)", step=0.001, key="pa")
            p_b = st.number_input("Price B (Public)", step=0.001, key="pb")
            p_add = st.number_input("Additional Price (Public)", step=0.001, key="padd")

        if st.button("Save Public Price"):
            new_row = pd.DataFrame([{
                "Company": company,
                "Start Time": p_start.strftime("%H:%M:%S"),
                "End Time": p_end.strftime("%H:%M:%S"),
                "Price A": p_a,
                "Price B": p_b,
                "Additional Price": p_add
            }])
            if company not in public_prices["Company"].values:
                public_prices = pd.concat([public_prices, new_row], ignore_index=True)
            write_csv_s3(public_prices, PUBLIC_PRICE_FILE)
            st.success("Public price saved!")
            st.cache_data.clear()



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8501))
    os.system(f"streamlit run app.py --server.port {port} --server.address 0.0.0.0")


