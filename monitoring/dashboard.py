import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os
import random
from pathlib import Path
from pipeline_monitor import PipelineMonitor
from alert_manager import AlertManager

# Initialize session state for alerts if not exists
if 'alerts' not in st.session_state:
    st.session_state.alerts = []

# Set page configuration
st.set_page_config(
    page_title="Data Pipeline Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for KPI dashboard
st.markdown("""
<style>
    .kpi-container {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
        gap: 2rem;
        margin: 2rem;
    }
    .kpi-card {
        background-color: #262730;
        border-radius: 0.75rem;
        padding: 3rem;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.2);
        border: 1px solid rgba(255, 255, 255, 0.2);
    }
    .kpi-label {
        color: #FFFFFF;
        font-size: 1.2rem;
        font-weight: 600;
        text-transform: uppercase;
        margin-bottom: 1rem;
        letter-spacing: 0.05em;
    }
    .kpi-value {
        color: #FFFFFF;
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 1rem;
        font-family: 'Arial', sans-serif;
    }
    .kpi-trend {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        font-size: 1.1rem;
        font-weight: 500;
        margin-top: 1rem;
    }
    .kpi-trend-positive {
        color: #00FF9D;
        background: rgba(0, 255, 157, 0.15);
        padding: 0.75rem 1.25rem;
        border-radius: 0.5rem;
    }
    .kpi-trend-negative {
        color: #FF5B5B;
        background: rgba(255, 91, 91, 0.15);
        padding: 0.75rem 1.25rem;
        border-radius: 0.5rem;
    }
    .kpi-trend-neutral {
        color: #E6E9F0;
        background: rgba(230, 233, 240, 0.15);
        padding: 0.75rem 1.25rem;
        border-radius: 0.5rem;
    }
    .kpi-subtitle {
        color: #E6E9F0;
        font-size: 1rem;
        margin-top: 0.75rem;
        font-weight: 500;
    }
    /* Main container padding */
    .block-container {
        padding: 3rem 2rem;
    }
    /* Header styling */
    h1 {
        color: #FFFFFF;
        font-size: 2.5rem !important;
        font-weight: 700 !important;
        margin: 2rem 0 !important;
    }
    .footer {
        position: fixed;
        bottom: 0;
        left: 0;
        width: 100%;
        background-color: rgba(28, 31, 38, 0.95);
        padding: 10px 20px;
        text-align: center;
        border-top: 1px solid rgba(250, 250, 250, 0.1);
    }
    .author-info {
        color: #FAFAFA;
        font-size: 14px;
        margin: 5px 0;
    }
    .github-link {
        color: #00ADB5;
        text-decoration: none;
        margin-left: 10px;
    }
    .github-link:hover {
        color: #4DD0E1;
        text-decoration: underline;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.title("📊 Data Pipeline Monitor")
st.markdown("""
This dashboard provides real-time monitoring of your data pipelines and their performance metrics.
""")
# Add footer with author information and GitHub link
st.markdown("""
<div class="footer">
    <p class="author-info">
        Created by MOHAMED BOUCHALKHA 
        <a href="https://github.com/MBouchalkha94/Data-Pipeline-Monitor" target="_blank" class="github-link">
            View on GitHub
        </a>
    </p>
</div>
""", unsafe_allow_html=True)
# Sidebar configuration
st.sidebar.header("Dashboard Controls")

# Alert configuration
st.sidebar.subheader("Alert Configuration")
alert_thresholds = {
    'success_rate': st.sidebar.slider(
        "Success Rate Threshold (%)", 
        min_value=0, 
        max_value=100, 
        value=95,
        help="Alert when success rate falls below this threshold"
    ),
    'duration': st.sidebar.number_input(
        "Max Duration Threshold (ms)",
        min_value=100,
        value=1000,
        help="Alert when pipeline duration exceeds this threshold"
    ),
    'memory_usage': st.sidebar.number_input(
        "Max Memory Usage (MB)",
        min_value=100,
        value=800,
        help="Alert when memory usage exceeds this threshold"
    ),
    'cpu_usage': st.sidebar.slider(
        "CPU Usage Threshold (%)",
        min_value=0,
        max_value=100,
        value=80,
        help="Alert when CPU usage exceeds this threshold"
    )
}

# Data source selector
data_source = st.sidebar.radio(
    "Select Data Source",
    ["Mock Data", "Azure Data Factory"],
    help="Choose between mock data for testing or real Azure Data Factory data"
)

# Date range selector
date_range = st.sidebar.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=7), datetime.now()),
    max_value=datetime.now()
)

# Refresh rate selector
refresh_rate = st.sidebar.selectbox(
    "Dashboard Refresh Rate",
    ["1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"],
    help="Select how often the dashboard should refresh"
)

def get_azure_pipeline_data():
    try:
        # Initialize PipelineMonitor with environment variables
        monitor = PipelineMonitor(
            subscription_id=os.getenv('AZURE_SUBSCRIPTION_ID'),
            resource_group=os.getenv('AZURE_RESOURCE_GROUP'),
            data_factory_name=os.getenv('AZURE_DATA_FACTORY_NAME')
        )
        
        # Get pipeline runs for the selected date range
        start_time = datetime.combine(date_range[0], datetime.min.time())
        end_time = datetime.combine(date_range[1], datetime.max.time())
        
        pipeline_runs = monitor.get_pipeline_runs(start_time, end_time)
        return pd.DataFrame(pipeline_runs)
    except Exception as e:
        st.error(f"Error connecting to Azure Data Factory: {str(e)}")
        return None

def generate_mock_pipeline_data():
    pipelines = ['ETL_Customer_Data', 'Load_Sales_Data', 'Update_Inventory', 'Generate_Reports']
    data = []
    
    start_date = date_range[0]
    end_date = date_range[1]
    current_date = start_date
    
    while current_date <= end_date:
        for pipeline in pipelines:
            # Create multiple runs per day
            for _ in range(random.randint(1, 4)):
                timestamp = pd.Timestamp(current_date) + timedelta(
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59)
                )
                status = 'Success' if random.random() > 0.1 else 'Failed'
                duration = random.randint(100, 1000)
                data.append({
                    'pipeline_name': pipeline,
                    'status': status,
                    'start_time': timestamp,
                    'duration_in_ms': duration,
                    'records_processed': random.randint(1000, 10000),
                    'memory_usage_mb': random.randint(100, 1000),
                    'cpu_usage_percent': random.randint(10, 90)
                })
        current_date += timedelta(days=1)
    
    df = pd.DataFrame(data)
    return df

def check_alerts(pipeline_data, thresholds):
    """Check for alert conditions and return alerts."""
    alerts = []
    
    # Check success rate
    success_rate = (pipeline_data['status'] == 'Success').mean() * 100
    if success_rate < thresholds['success_rate']:
        alerts.append({
            'level': 'ERROR',
            'title': 'Low Pipeline Success Rate',
            'message': f"Current success rate ({success_rate:.1f}%) is below threshold ({thresholds['success_rate']}%)"
        })
    
    # Check duration
    long_running = pipeline_data[pipeline_data['duration_in_ms'] > thresholds['duration']]
    if not long_running.empty:
        alerts.append({
            'level': 'WARNING',
            'title': 'Long Running Pipelines Detected',
            'message': f"{len(long_running)} pipeline(s) exceeded duration threshold of {thresholds['duration']}ms"
        })
    
    # Check memory usage
    high_memory = pipeline_data[pipeline_data['memory_usage_mb'] > thresholds['memory_usage']]
    if not high_memory.empty:
        alerts.append({
            'level': 'WARNING',
            'title': 'High Memory Usage Detected',
            'message': f"{len(high_memory)} pipeline(s) exceeded memory threshold of {thresholds['memory_usage']}MB"
        })
    
    # Check CPU usage
    high_cpu = pipeline_data[pipeline_data['cpu_usage_percent'] > thresholds['cpu_usage']]
    if not high_cpu.empty:
        alerts.append({
            'level': 'WARNING',
            'title': 'High CPU Usage Detected',
            'message': f"{len(high_cpu)} pipeline(s) exceeded CPU threshold of {thresholds['cpu_usage']}%"
        })
    
    return alerts

# Load pipeline data based on selected source
if data_source == "Azure Data Factory":
    pipeline_data = get_azure_pipeline_data()
    if pipeline_data is None:
        pipeline_data = generate_mock_pipeline_data()
        st.warning("Failed to connect to Azure Data Factory. Using mock data instead.")
else:
    pipeline_data = generate_mock_pipeline_data()

# Check for alerts
current_alerts = check_alerts(pipeline_data, alert_thresholds)
if current_alerts:
    st.session_state.alerts.extend(current_alerts)

# Display alerts if any
if st.session_state.alerts:
    with st.expander("🚨 Active Alerts", expanded=True):
        for idx, alert in enumerate(st.session_state.alerts):
            alert_color = "red" if alert['level'] == "ERROR" else "orange"
            st.markdown(
                f"""
                <div style='padding: 10px; border-left: 5px solid {alert_color}; background-color: {alert_color}22;'>
                <h4 style='color: {alert_color}; margin: 0;'>{alert['title']}</h4>
                <p style='margin: 5px 0;'>{alert['message']}</p>
                </div>
                """,
                unsafe_allow_html=True
            )
        if st.button("Clear Alerts"):
            st.session_state.alerts = []
            st.rerun()

# Top-level metrics
st.header("📈 Key Performance Indicators")

# Calculate metrics and trends
success_rate = (pipeline_data['status'] == 'Success').mean() * 100
prev_success_rate = 95  # Example previous value
success_rate_change = success_rate - prev_success_rate

avg_duration = pipeline_data['duration_in_ms'].mean() / 1000  # Convert to seconds
prev_avg_duration = 12  # Example previous value
duration_change = ((avg_duration - prev_avg_duration) / prev_avg_duration) * 100

total_runs = len(pipeline_data)
prev_total_runs = 5000  # Example previous value
runs_change = ((total_runs - prev_total_runs) / prev_total_runs) * 100

total_records = pipeline_data['records_processed'].sum()
prev_total_records = 5000  # Example previous value
records_change = ((total_records - prev_total_records) / prev_total_records) * 100

# Calculate additional metrics
failed_runs = len(pipeline_data[pipeline_data['status'] == 'Failed'])
failure_rate = (failed_runs / total_runs) * 100

avg_records_per_run = total_records / total_runs if total_runs > 0 else 0
peak_duration = pipeline_data['duration_in_ms'].max() / 1000  # Convert to seconds

# Time-based metrics
current_time = pd.Timestamp.now()
last_24h_data = pipeline_data[pipeline_data['start_time'] > (current_time - pd.Timedelta(days=1))]
runs_last_24h = len(last_24h_data)
success_rate_24h = (last_24h_data['status'] == 'Success').mean() * 100 if not last_24h_data.empty else 0

# Create KPI grid with 2 rows
row1_cols = st.columns(4)
row2_cols = st.columns(4)

# Row 1: Primary KPIs
with row1_cols[0]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">✓</div>
        <div class="kpi-label">Success Rate</div>
        <div class="kpi-value">{success_rate:.1f}%</div>
        <div class="kpi-subtitle">Last 24h: {success_rate_24h:.1f}%</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'positive' if success_rate_change >= 0 else 'negative'}">
                {'↑' if success_rate_change >= 0 else '↓'} {abs(success_rate_change):.1f}%
            </span>
            <span>vs last period</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row1_cols[1]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">⚡</div>
        <div class="kpi-label">Average Duration</div>
        <div class="kpi-value">{avg_duration:.1f}s</div>
        <div class="kpi-subtitle">Peak: {peak_duration:.1f}s</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'negative' if duration_change >= 0 else 'positive'}">
                {'↑' if duration_change >= 0 else '↓'} {abs(duration_change):.1f}%
            </span>
            <span>vs last period</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row1_cols[2]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">🔄</div>
        <div class="kpi-label">Total Pipeline Runs</div>
        <div class="kpi-value">{total_runs:,}</div>
        <div class="kpi-subtitle">Last 24h: {runs_last_24h:,}</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'positive' if runs_change >= 0 else 'negative'}">
                {'↑' if runs_change >= 0 else '↓'} {abs(runs_change):.1f}%
            </span>
            <span>vs last period</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row1_cols[3]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">📊</div>
        <div class="kpi-label">Records Processed</div>
        <div class="kpi-value">{total_records:,}</div>
        <div class="kpi-subtitle">Avg: {avg_records_per_run:,.0f}/run</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'positive' if records_change >= 0 else 'negative'}">
                {'↑' if records_change >= 0 else '↓'} {abs(records_change):.1f}%
            </span>
            <span>vs last period</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Row 2: Secondary KPIs
with row2_cols[0]:
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">❌</div>
        <div class="kpi-label">Failed Runs</div>
        <div class="kpi-value">{failed_runs:,}</div>
        <div class="kpi-subtitle">Failure Rate: {failure_rate:.1f}%</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'negative' if failure_rate > 5 else 'positive'}">
                {failure_rate:.1f}% of total runs
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row2_cols[1]:
    # Calculate resource metrics
    avg_memory = pipeline_data['memory_usage_mb'].mean()
    peak_memory = pipeline_data['memory_usage_mb'].max()
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">💾</div>
        <div class="kpi-label">Memory Usage</div>
        <div class="kpi-value">{avg_memory:.0f} MB</div>
        <div class="kpi-subtitle">Peak: {peak_memory:.0f} MB</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'negative' if peak_memory > 800 else 'positive'}">
                {((peak_memory - avg_memory) / avg_memory * 100):.1f}% peak increase
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row2_cols[2]:
    # Calculate CPU metrics
    avg_cpu = pipeline_data['cpu_usage_percent'].mean()
    peak_cpu = pipeline_data['cpu_usage_percent'].max()
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">⚙️</div>
        <div class="kpi-label">CPU Usage</div>
        <div class="kpi-value">{avg_cpu:.1f}%</div>
        <div class="kpi-subtitle">Peak: {peak_cpu:.1f}%</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'negative' if peak_cpu > 80 else 'positive'}">
                {((peak_cpu - avg_cpu) / avg_cpu * 100):.1f}% peak increase
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

with row2_cols[3]:
    # Calculate throughput
    try:
        latest_time = pipeline_data['start_time'].max()
        earliest_time = pipeline_data['start_time'].min()
        time_range = (latest_time - earliest_time).total_seconds()
        throughput = total_records / time_range if time_range > 0 else 0
    except Exception:
        throughput = 0
        time_range = 0
    
    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">⚡</div>
        <div class="kpi-label">Throughput</div>
        <div class="kpi-value">{throughput:,.0f}</div>
        <div class="kpi-subtitle">records/second</div>
        <div class="kpi-trend">
            <span class="kpi-trend-{'positive' if throughput > 100 else 'neutral'}">
                {runs_last_24h:,} runs in last 24h
            </span>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Create dashboard layout
st.header("📊 Pipeline Analytics")
tab1, tab2, tab3, tab4 = st.tabs(["Status & Performance", "Resource Usage", "Detailed Analysis", "Alert Management"])

with tab1:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Pipeline Status Overview")
        status_counts = pipeline_data['status'].value_counts()
        fig_status = px.pie(
            values=status_counts.values,
            names=status_counts.index,
            title="Pipeline Execution Status",
            color_discrete_map={'Success': '#00CC96', 'Failed': '#EF553B'}
        )
        st.plotly_chart(fig_status)

    with col2:
        st.subheader("Duration Analysis")
        fig_duration = px.box(
            pipeline_data,
            x='pipeline_name',
            y='duration_in_ms',
            color='status',
            title="Pipeline Duration Distribution by Status"
        )
        st.plotly_chart(fig_duration)

with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Memory Usage Trends")
        fig_memory = px.line(
            pipeline_data.groupby('start_time')['memory_usage_mb'].mean().reset_index(),
            x='start_time',
            y='memory_usage_mb',
            title="Average Memory Usage Over Time"
        )
        st.plotly_chart(fig_memory)
    
    with col2:
        st.subheader("CPU Usage Distribution")
        fig_cpu = px.histogram(
            pipeline_data,
            x='cpu_usage_percent',
            nbins=20,
            title="CPU Usage Distribution"
        )
        st.plotly_chart(fig_cpu)

with tab3:
    st.subheader("Pipeline Performance Trends")
    # Group by hour for more detailed trend analysis
    hourly_performance = pipeline_data.groupby(
        [pd.Grouper(key='start_time', freq='H'), 'pipeline_name']
    )['duration_in_ms'].mean().reset_index()

    fig_trends = px.line(
        hourly_performance,
        x='start_time',
        y='duration_in_ms',
        color='pipeline_name',
        title="Hourly Average Pipeline Duration"
    )
    st.plotly_chart(fig_trends, use_container_width=True)

    # Add success rate by pipeline
    success_by_pipeline = (
        pipeline_data.groupby('pipeline_name')
        .agg({
            'status': lambda x: (x == 'Success').mean() * 100,
            'duration_in_ms': 'mean',
            'records_processed': 'sum'
        })
        .round(2)
        .reset_index()
    )
    success_by_pipeline.columns = ['Pipeline Name', 'Success Rate (%)', 'Avg Duration (ms)', 'Total Records']
    
    st.subheader("Pipeline-wise Performance Summary")
    st.dataframe(
        success_by_pipeline.style.format({
            'Success Rate (%)': '{:.1f}%',
            'Avg Duration (ms)': '{:.0f}',
            'Total Records': '{:,.0f}'
        })
    )

with tab4:
    st.subheader("Alert Management")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Current Alert Thresholds")
        st.json(alert_thresholds)
    
    with col2:
        st.markdown("### Alert History")
        if st.session_state.alerts:
            alert_df = pd.DataFrame(st.session_state.alerts)
            st.dataframe(alert_df)
        else:
            st.info("No alerts in history")
    
    # Alert statistics
    if st.session_state.alerts:
        alert_stats = pd.DataFrame(st.session_state.alerts).groupby('level').size()
        fig_alerts = px.pie(
            values=alert_stats.values,
            names=alert_stats.index,
            title="Alert Distribution by Level",
            color_discrete_map={'ERROR': '#EF553B', 'WARNING': '#FFA15A'}
        )
        st.plotly_chart(fig_alerts)

# Failed Pipeline Analysis
st.header("❌ Failed Pipeline Analysis")
failed_pipelines = pipeline_data[pipeline_data['status'] == 'Failed'].sort_values('start_time', ascending=False)

if not failed_pipelines.empty:
    # Failed pipeline trends
    failed_trends = (
        pipeline_data[pipeline_data['status'] == 'Failed']
        .groupby([pd.Grouper(key='start_time', freq='D'), 'pipeline_name'])
        .size()
        .reset_index(name='failure_count')
    )
    
    fig_failures = px.bar(
        failed_trends,
        x='start_time',
        y='failure_count',
        color='pipeline_name',
        title="Daily Pipeline Failures"
    )
    st.plotly_chart(fig_failures, use_container_width=True)
    
    st.subheader("Recent Failed Runs")
    display_columns = ['pipeline_name', 'start_time', 'duration_in_ms', 'records_processed', 'memory_usage_mb', 'cpu_usage_percent']
    st.dataframe(
        failed_pipelines[display_columns].style.format({
            'duration_in_ms': '{:.0f}',
            'records_processed': '{:,.0f}',
            'start_time': lambda x: x.strftime('%Y-%m-%d %H:%M:%S'),
            'memory_usage_mb': '{:.0f}',
            'cpu_usage_percent': '{:.1f}%'
        })
    )
else:
    st.success("No failed pipeline runs in the selected date range!")

# Add auto-refresh capability
if st.button("Refresh Dashboard"):
    st.rerun()

# Add footer with author information and GitHub link
st.markdown("""
<div class="footer">
    <p class="author-info">
        Created by MOHAMED BOUCHALKHA 
        <a href="https://github.com/MBouchalkha94/Data-Pipeline-Monitor" target="_blank" class="github-link">
            <img src="https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png" width="20" style="vertical-align: middle;"> View on GitHub
        </a>
    </p>
</div>
""", unsafe_allow_html=True)

# Add refresh rate information
st.sidebar.info(f"Dashboard will auto-refresh every {refresh_rate}")
