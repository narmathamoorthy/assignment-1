# importing pandas
import pandas as pd
# read csv file 
df= pd.read_csv("C:\\Users\\narmathm\\Downloads\\traffic_stops - traffic_stops_with_vehicle_number.csv")
print(df.head())
#change col names to upper 
df.columns = df.columns.str.upper()
# remove coloumns that are completely empty 
df = df.dropna(axis=1, how='all')
print (df.columns)
# convert STOP_DATE, STOP_TIME columns to correct types
df['STOP_DATE'] = pd.to_datetime(df['STOP_DATE']).dt.date
df['STOP_TIME'] = pd.to_datetime(df['STOP_TIME']).dt.time


# creating table and connecting mysql and pandas 
from sqlalchemy import create_engine, text
engine = create_engine('mysql+pymysql://root:9789749089@localhost/mydatabase')
create_table_sql = """
CREATE TABLE IF NOT EXISTS traffic_stops (
    stop_id INT PRIMARY KEY AUTO_INCREMENT,
    stop_date DATE,
    stop_time TIME,
    country_name VARCHAR(50),
    driver_gender VARCHAR(10),
    driver_age_raw INT,
    driver_age INT,
    driver_race VARCHAR(20),
    violation_raw VARCHAR(100),
    violation VARCHAR(100),
    search_conducted BOOLEAN,
    search_type VARCHAR(50),
    stop_outcome VARCHAR(50),
    is_arrested BOOLEAN,
    stop_duration VARCHAR(20),
    drugs_related_stop BOOLEAN,
    vehicle_number VARCHAR(20)
);
"""
# connect engine with table 
with engine.connect() as nm:
    nm.execute(text(create_table_sql))
    print("Table 'traffic_stops' created successfully.")

# inset values to tables 
df.to_sql('traffic_stops', con=engine, if_exists='append', index=False)
print("Data inserted successfully.")
#connecting my mysql and python using engine 
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM traffic_stops LIMIT 10"))
    for row in result:
        print(row)
# importing streamlit 

import streamlit as st
import pandas as pd
df_streamlit = pd.read_sql('SELECT * FROM traffic_stops', con=engine)

st.title(" :bar_chart: Secure Check Traffic Stop Dashboard")
st.markdown("<h1 style='font-size:11px;'>Your Custom Title</h1>", unsafe_allow_html=True)

import streamlit as st
import pandas as pd
import altair as alt
from sqlalchemy import text

st.title("Advanced Insights - Traffic Stop Analytics Dashboard")

insight_options = [
    "Vehicle-Based Analysis: Top 10 drug-related vehicles",
    "Vehicle-Based Analysis: Most frequently searched vehicles",
    "Arrest Rates by Driver Age Group",
    "Gender Distribution by Country",
    "Race and Gender Wise Arrest Rate",
    "Most Traffic Stops by Hour",
    "Average Stop Duration by Violation",
    "Arrest Analysis by Time Period (Day/Night)",
    "Violations Most Associated with Searches or Arrests",
    "Violations Among Younger Drivers (<25)",
    "Violations Rarely Resulting in Search or Arrest",
    "Countries with Highest Drug-Related Stops",
    "Arrest Rate by Country and Violation",
    "Countries with Most Stops with Search Conducted",
    "Yearly Breakdown of Stops and Arrests by Country",
    "Driver Violation Trends by Age and Race",
    "Time Period Analysis of Stops (Year/Month/Hour)",
    "Violations with High Search and Arrest Rates",
    "Driver Demographics by Country (Age/Gender/Race)",
    "Top 5 Violations with Highest Arrest Rates"
]

selection = st.selectbox("Select an Advanced Insight to View", insight_options)

# Define a function to run a query and return a dataframe
def run_query(q):
    return pd.read_sql(text(q), con=engine)

# Vehicle Queries
query_top_vehicles = """
SELECT vehicle_number, COUNT(*) AS drug_related_stop_count
FROM traffic_stops
WHERE drugs_related_stop = TRUE
GROUP BY vehicle_number
ORDER BY drug_related_stop_count DESC
LIMIT 10;
"""

query_most_searched_vehicles = """
SELECT vehicle_number, COUNT(*) AS search_count
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY vehicle_number
ORDER BY search_count DESC
LIMIT 10;
"""

# Other queries as given (not all repeated here to save space) ...
query_arrest_age_group = """
SELECT 
  CASE 
    WHEN driver_age < 18 THEN '<18'
    WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
    WHEN driver_age BETWEEN 25 AND 35 THEN '25-35'
    WHEN driver_age BETWEEN 36 AND 45 THEN '36-45'
    ELSE '45+'
  END AS age_group,
  COUNT(*) AS total_count,
  SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_sum,
  ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
FROM traffic_stops
GROUP BY age_group
ORDER BY arrest_rate DESC;
"""

query_gender_dist = """
SELECT country_name, driver_gender, COUNT(*) AS total_count
FROM traffic_stops
GROUP BY country_name, driver_gender
ORDER BY total_count DESC;
"""

query_race_gender_arrest = """
SELECT driver_race, driver_gender, COUNT(*) AS total_count,
SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrested_sum,
ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrested_rate
FROM traffic_stops
GROUP BY driver_race, driver_gender
ORDER BY arrested_rate DESC;
"""

query_most_traffic_hour = """
SELECT HOUR(stop_time) AS stop_hour, COUNT(*) AS total_stops
FROM traffic_stops
GROUP BY stop_hour
ORDER BY total_stops DESC
LIMIT 1;
"""

query_avg_duration = """
SELECT violation, AVG(CAST(stop_duration AS UNSIGNED)) AS average_duration
FROM traffic_stops
GROUP BY violation
ORDER BY average_duration;
"""

query_arrest_by_time_period = """
SELECT 
  CASE 
    WHEN HOUR(stop_time) BETWEEN 22 AND 23 OR HOUR(stop_time) BETWEEN 0 AND 5 THEN 'night'
    ELSE 'day'
  END AS time_period,
  COUNT(*) AS total_stops,
  SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_count,
  ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS avg_arrest_rate
FROM traffic_stops
GROUP BY time_period;
"""

query_search_arrest_assoc = """
SELECT violation AS Violation, total_count AS Total_Count, Arrest_avg, Search_avg
FROM (SELECT violation, COUNT(*) AS total_count,
      SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_sum,
      ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS Arrest_avg,
      SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS search_sum,
      ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS Search_avg
      FROM traffic_stops GROUP BY violation) t
ORDER BY Arrest_avg DESC, Search_avg DESC;
"""

query_young_violations = """
SELECT violation AS Violation, COUNT(*) AS Total_Count
FROM traffic_stops
WHERE driver_age < 25
GROUP BY violation
ORDER BY Total_Count DESC;
"""

query_rare_violations = """
SELECT violation AS Violation, total_count AS Total_Count, Arrest_avg, Search_avg
FROM (SELECT violation, COUNT(*) AS total_count,
      SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_sum,
      ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS Arrest_avg,
      SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS search_sum,
      ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS Search_avg
      FROM traffic_stops GROUP BY violation) t
ORDER BY Arrest_avg, Search_avg;
"""

query_drug_related_by_country = """
SELECT country_name, COUNT(*) AS total_count,
SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) AS drug_related_stop,
ROUND(100.0 * SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS avg_drug_related_stop
FROM traffic_stops
GROUP BY country_name
ORDER BY avg_drug_related_stop DESC;
"""

query_arrest_by_country_violation = """
SELECT violation AS Violation, country_name AS Country, COUNT(*) AS Total_Count,
SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS Sum_Arrest,
ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS Avg_Arrest
FROM traffic_stops
GROUP BY Country, Violation
ORDER BY Avg_Arrest DESC;
"""

query_search_stops_by_country = """
SELECT country_name AS Country, COUNT(*) AS Total_Stops
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY Country
ORDER BY Total_Stops DESC;
"""

query_yearly_breakdown = """
SELECT _Year_, total_stops, SUM(total_arrests) OVER (PARTITION BY Country ORDER BY _Year_) AS cum_arrest
FROM (SELECT country_name AS Country, YEAR(stop_date) AS _Year_, COUNT(*) AS total_stops,
      SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
      ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
      FROM traffic_stops GROUP BY Country, _Year_ ORDER BY arrest_rate) yearly_data
ORDER BY Country, _Year_;
"""

query_driver_trends = """
SELECT violation_raw, driver_race, age_group, COUNT(*) AS violation_count
FROM (SELECT violation_raw, driver_race,
      CASE WHEN driver_age < 18 THEN '<18'
           WHEN driver_age BETWEEN 18 AND 24 THEN '18-24'
           WHEN driver_age BETWEEN 25 AND 34 THEN '25-34'
           WHEN driver_age BETWEEN 35 AND 44 THEN '35-44'
           ELSE '45+' END AS age_group
      FROM traffic_stops) t
GROUP BY violation_raw, driver_race, age_group
ORDER BY driver_race, age_group;
"""

query_time_period_analysis = """
SELECT COUNT(*) AS total_stops, YEAR(stop_date) AS yearly, MONTH(stop_date) AS monthly, HOUR(stop_time) AS hourly
FROM traffic_stops
GROUP BY yearly, monthly, hourly
ORDER BY yearly, monthly, hourly;
"""

query_high_search_arrest = """
SELECT violation_raw, arrest_percent, search_percent
FROM (SELECT violation_raw, COUNT(*) AS total_stops,
      SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrested_count,
      ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_percent,
      SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS search_count,
      ROUND(100.0 * SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS search_percent
      FROM traffic_stops GROUP BY violation_raw) t
ORDER BY arrest_percent DESC, search_percent DESC;
"""

query_driver_demographics = """
SELECT country_name, driver_gender, driver_race, COUNT(*) AS driver_count
FROM traffic_stops
GROUP BY country_name, driver_gender, driver_race
ORDER BY country_name, driver_gender, driver_race;
"""

query_top_arrest_violations = """
SELECT violation_raw, COUNT(*) AS total_stops,
       SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS arrest_sum,
       ROUND(100.0 * SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS arrest_rate
FROM traffic_stops
GROUP BY violation_raw
ORDER BY arrest_rate DESC
LIMIT 5;
"""

# Logic to run the selected query and display results accordingly

if selection == insight_options[0]:
    st.subheader("Top 10 Vehicles Involved in Drug-Related Stops")
    df = run_query(query_top_vehicles)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[1]:
    st.subheader("Vehicles Most Frequently Searched")
    df = run_query(query_most_searched_vehicles)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[2]:
    st.subheader("Arrest Rates by Driver Age Group")
    df = run_query(query_arrest_age_group)
    chart = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X('age_group:N', axis=alt.Axis(title='Age Group')),
        y=alt.Y('arrest_rate:Q', axis=alt.Axis(title='Arrest Rate (%)')),
        tooltip=[alt.Tooltip('age_group', title='Age Group'), alt.Tooltip('arrest_rate', title='Arrest Rate (%)')]
    ).properties(title='Arrest Rate by Driver Age Group')
    st.altair_chart(chart, use_container_width=True)
elif selection == insight_options[3]:
    st.subheader("Gender Distribution of Drivers Stopped by Country")
    df = run_query(query_gender_dist)
    df.insert(0, 'Row Number', range(1, len(df)+1))
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[4]:
    st.subheader("Race and Gender Wise Arrest Rate")
    df = run_query(query_race_gender_arrest)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[5]:
    st.subheader('Most Traffic Stops by Hour of Day')
    df = run_query(query_most_traffic_hour)
    df.insert(0, 'Row Number', range(1, len(df)+1))
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[6]:
    st.header("Average Stop Duration for Different Violations (in Minutes)")
    df = run_query(query_avg_duration)
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('violation:N', title='Violation'),
        y=alt.Y('average_duration:Q', title='Average Duration (minutes)'),
        tooltip=['violation', 'average_duration']
    ).properties(
        width=400, height=400,
        title="Average Stop Duration by Violation"
    ).configure_axis(labelAngle=-45)
    st.altair_chart(chart, use_container_width=True)
elif selection == insight_options[7]:
    st.header("Arrest Analysis by Time Period (Day/Night)")
    df = run_query(query_arrest_by_time_period)
    df.insert(0, 'Row Number', range(1, len(df)+1))
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[8]:
    st.header("Violations Most Associated with Searches or Arrests")
    df = run_query(query_search_arrest_assoc)
    metric = st.selectbox("Select metric to visualize", ['Arrest_avg', 'Search_avg'])
    chart_data = df[['Violation', metric]].rename(columns={metric: 'value'})
    pie_chart = alt.Chart(chart_data).mark_arc().encode(
        theta=alt.Theta(field="value", type="quantitative"),
        color=alt.Color(field="Violation", type="nominal"),
        tooltip=['Violation', 'value']
    ).properties(width=400, height=400, title=f"Pie Chart of {metric} by Violation")
    st.altair_chart(pie_chart, use_container_width=True)
elif selection == insight_options[9]:
    st.subheader("Violations Among Younger Drivers (<25)")
    df = run_query(query_young_violations)
    df.insert(0, 'Row Number', range(1, len(df)+1))
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[10]:
    st.subheader("Violations Rarely Resulting in Search or Arrest")
    df = run_query(query_rare_violations)
    df.insert(0, 'Row Number', range(1, len(df)+1))
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[11]:
    st.subheader("Countries with Highest Rate of Drug-Related Stops")
    df = run_query(query_drug_related_by_country)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[12]:
    st.subheader("Arrest Rate by Country and Violation")
    df = run_query(query_arrest_by_country_violation)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[13]:
    st.subheader("Countries with the Most Stops where Search Conducted")
    df = run_query(query_search_stops_by_country)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[14]:
    st.subheader("Yearly Breakdown of Stops and Arrests by Country")
    df = run_query(query_yearly_breakdown)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[15]:
    st.subheader("Driver Violation Trends by Age and Race")
    df = run_query(query_driver_trends)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[16]:
    st.subheader("Time Period Analysis of Stops")
    df = run_query(query_time_period_analysis)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[17]:
    st.subheader("Violations with High Search and Arrest Rates")
    df = run_query(query_high_search_arrest)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[18]:
    st.subheader("Driver Demographics by Country")
    df = run_query(query_driver_demographics)
    st.dataframe(df, use_container_width=True)
elif selection == insight_options[19]:
    st.subheader("Top 5 Violations with Highest Arrest Rates")
    df = run_query(query_top_arrest_violations)
    st.dataframe(df, use_container_width=True)
query_vdr = """
SELECT 
    ROUND(
        100.0 * COUNT(*) 
        / NULLIF(
            (SELECT COUNT(*) 
             FROM traffic_stops 
             WHERE drugs_related_stop = TRUE),
            0
        ),
        2
    ) AS violation_detection_rate
FROM traffic_stops
WHERE drugs_related_stop = TRUE;
"""

df_vdr = pd.read_sql(query_vdr, con=engine)
rate = float(df_vdr["violation_detection_rate"].iloc[0] or 0)
st.metric("Violation Detection Rate (%)", f"{rate:.2f}")

st.title("High-Risk Vehicle Analytics")

# Query top 10 vehicles by number of violations
query_top_violations = """
SELECT vehicle_number, COUNT(*) AS violation_count
FROM traffic_stops
GROUP BY vehicle_number
ORDER BY violation_count DESC
LIMIT 10;
"""

# Query top 10 vehicles by number of arrests
query_top_arrests = """
SELECT vehicle_number, COUNT(*) AS arrest_count
FROM traffic_stops
WHERE is_arrested = TRUE
GROUP BY vehicle_number
ORDER BY arrest_count DESC
LIMIT 10;
"""

# Query top 10 vehicles by number of searches
query_top_searches = """
SELECT vehicle_number, COUNT(*) AS search_count
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY vehicle_number
ORDER BY search_count DESC
LIMIT 10;
"""

option = st.selectbox(
    "Select high-risk vehicle metric to analyze:",
    ["Most Violations", "Most Arrests", "Most Searches"]
)

if option == "Most Violations":
    df = pd.read_sql(text(query_top_violations), con=engine)
    st.subheader("Top 10 Vehicles with Most Violations")
    st.dataframe(df)
    # Visualization
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('vehicle_number:N', title='Vehicle Number'),
        y=alt.Y('violation_count:Q', title='Number of Violations'),
        tooltip=['vehicle_number', 'violation_count']
    ).properties(width=700, height=400)
    st.altair_chart(chart, use_container_width=True)

elif option == "Most Arrests":
    df = pd.read_sql(text(query_top_arrests), con=engine)
    st.subheader("Top 10 Vehicles with Most Arrests")
    st.dataframe(df)
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('vehicle_number:N', title='Vehicle Number'),
        y=alt.Y('arrest_count:Q', title='Number of Arrests'),
        tooltip=['vehicle_number', 'arrest_count']
    ).properties(width=700, height=400)
    st.altair_chart(chart, use_container_width=True)

else:
    df = pd.read_sql(text(query_top_searches), con=engine)
    st.subheader("Top 10 Vehicles with Most Searches")
    st.dataframe(df)
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X('vehicle_number:N', title='Vehicle Number'),
        y=alt.Y('search_count:Q', title='Number of Searches'),
        tooltip=['vehicle_number', 'search_count']
    ).properties(width=700, height=400)
    st.altair_chart(chart, use_container_width=True)
    
    
import streamlit as st
import pandas as pd
from sqlalchemy import text

st.title("Traffic Stop Lookup Portal - Efficient Law Enforcement Insights")

# Input filters for Lookup
st.header("Search Vehicles and Violations")

vehicle_number = st.text_input("Enter Vehicle Number:").strip()
country = st.text_input("Enter Country Name:").strip()
min_age = st.number_input("Minimum Driver Age:", min_value=0, max_value=120, step=1, value=0)
max_age = st.number_input("Maximum Driver Age:", min_value=0, max_value=120, step=1, value=120)
search_conducted = st.selectbox("Search Conducted?", options=["Any", "Yes", "No"])

# Build dynamic query filtering with parameters
query = """
SELECT stop_date, stop_time, country_name, driver_gender, driver_age_raw, driver_age,
       driver_race, violation_raw, violation, search_conducted, search_type,
       stop_outcome, is_arrested, stop_duration, drugs_related_stop, vehicle_number
FROM traffic_stops
WHERE 1=1
"""

params = {}

if vehicle_number:
    query += " AND vehicle_number LIKE :vehicle_number"
    params["vehicle_number"] = f"%{vehicle_number}%"

if country:
    query += " AND country_name = :country_name"
    params["country_name"] = country

if min_age > 0:
    query += " AND driver_age >= :min_age"
    params["min_age"] = min_age

if max_age < 120:
    query += " AND driver_age <= :max_age"
    params["max_age"] = max_age

if search_conducted != "Any":
    query += " AND search_conducted = :search_conducted"
    params["search_conducted"] = True if search_conducted == "Yes" else False

query += " ORDER BY stop_date DESC, stop_time DESC LIMIT 100;"

# Execute query
df_results = pd.read_sql(text(query), con=engine, params=params)

# Automated alerts for flagged vehicles (e.g., drugs_related_stop or is_arrested)
flagged_vehicles = df_results[(df_results["drugs_related_stop"] == True) | (df_results["is_arrested"] == True)]

if not flagged_vehicles.empty:
    st.warning(f"Alert: {len(flagged_vehicles)} flagged vehicles detected in results!")

# Display results
st.subheader(f"Lookup Results ({len(df_results)} records found)")
st.dataframe(df_results, use_container_width=True)

# Summary statistics for real-time reporting and decision-making
st.header("Summary Analytics")

total_stops = df_results.shape[0]
total_arrests = df_results["is_arrested"].sum()
total_searches = df_results["search_conducted"].sum()
drug_related_count = df_results["drugs_related_stop"].sum()

st.write(f"- Total Stops: {total_stops}")
st.write(f"- Total Arrests: {total_arrests}")
st.write(f"- Total Searches Conducted: {total_searches}")
st.write(f"- Drug-Related Stops: {drug_related_count}")

