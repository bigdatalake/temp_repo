
"""

"""
import streamlit as st
import pandas as pd
import altair as alt
import networkx as nx
import matplotlib.pyplot as plt
from query_utils import fetch_data
import textwrap

# Sample DataFrame for testing
def get_sample_data():
    """Create a sample DataFrame for testing."""
    sample_data = {
        "job_id": ["001", "001", "001", "001", "001"] + ["002", "002", "002", "002", "002"] + ["003"] + ["004"],
        "business_date": ["2024-12-01", "2024-12-02", "2024-12-03", "2024-12-04", "2024-12-05"] + ["2024-12-01", "2024-12-02", "2024-12-03", "2024-12-04", "2024-12-05"] + ["2024-12-01"] + ["2024-12-01"],
        "status": ["Success", "Success", "Success", "Success", "Success"] + ["Success", "Success", "Success", "FAILED", "FAILED"] + ["FAILED"] + ["Success"],
        "start_time": ["2024-12-01 15:00:00", "2024-12-02 15:00:00", "2024-12-03 15:50:00", "2024-12-04 15:00:00", "2024-12-05 15:30:00"]  + ["2024-12-01 15:00:00", "2024-12-02 15:00:00", "2024-12-03 15:00:00", "2024-12-04 15:00:00", "2024-12-05 15:00:00"] + ["2024-12-01 18:00:00"] + ["2024-12-01 02:00:00"],
        "end_time": ["2024-12-01 15:10:00", "2024-12-02 15:15:00", "2024-12-03 16:10:00", "2024-12-04 15:12:00", "2024-12-05 15:41:00"] + ["2024-12-01 16:00:00", "2024-12-02 15:25:00", "2024-12-03 15:20:00", "2024-12-04 15:05:00", "2024-12-05 15:55:00"] + ["2024-12-05 19:01:00"] + ["2024-12-05 02:13:00"],
        "execution_time": [10, 15, 20, 12, 11] + [60, 25, 20, 5, 55] + [61] + [13]
    }
    return pd.DataFrame(sample_data)

# Dashboard Title
st.title("ETL JOB Execution Monitoring Dashboard")

# Development Mode Toggle
dev_mode = st.sidebar.checkbox("Use Sample Data for Testing", value=False)

# Tabs for main dashboard and dependency graph
tab1, tab2 = st.tabs(["Main Dashboard", "Job Dependency Graph"])

with tab1:
    # Query Options
    QUERY_OPTIONS = {
        "By Business Date": "SELECT * FROM your_table WHERE business_date = ?",
        "By Business Date and Job ID": "SELECT * FROM your_table WHERE business_date = ? AND job_id = ?",
        "By Business Date Range": "SELECT * FROM your_table WHERE business_date BETWEEN ? AND ?"
    }

    # Get Data Function
    def get_data(query_type, params):
        if dev_mode:
            df = get_sample_data()
            if query_type == "By Business Date":
                filtered_df = df[df["business_date"] == params[0]]
            elif query_type == "By Business Date and Job ID":
                filtered_df = df[(df["business_date"] == params[0]) & (df["job_id"] == params[1])]
            elif query_type == "By Business Date Range":
                from_date, to_date = params
                filtered_df = df[(df["business_date"] >= from_date) & (df["business_date"] <= to_date)]
            else:
                filtered_df = df
            return filtered_df if not filtered_df.empty else pd.DataFrame()  # Return empty DataFrame if no data
        else:
            query = QUERY_OPTIONS[query_type]
            data, columns = fetch_data(query, params)
            return pd.DataFrame(data, columns=columns) if data else pd.DataFrame()  # Return empty DataFrame if no data

    # Sidebar Inputs for Main Dashboard
    #st.sidebar.markdown("#### Dashboard Inputs")  # Add a title for this section
    st.sidebar.markdown(" #### <u>Dashboard Inputs</u>", unsafe_allow_html=True)


    # Sidebar Inputs
    query_type = st.sidebar.radio("Select Query Type", list(QUERY_OPTIONS.keys()))

    # Form Inputs
    if query_type == "By Business Date":
        business_date = st.sidebar.date_input("Business Date")
        params = [str(business_date)]
    elif query_type == "By Business Date and Job ID":
        business_date = st.sidebar.date_input("Business Date")
        record_id = st.sidebar.text_input("Job ID")
        params = [str(business_date), record_id]
    elif query_type == "By Business Date Range":
        from_date = st.sidebar.date_input("From Date")
        to_date = st.sidebar.date_input("To Date")
        params = [str(from_date), str(to_date)]

    # Fetch Data
    if st.sidebar.button("Fetch Data"):
        st.session_state.data = get_data(query_type, params)

    # Process and Display Data
    if "data" in st.session_state and st.session_state.data is not None:
        data = st.session_state.data

        if data.empty:  # Check if the DataFrame is empty
            st.warning("No data available for the selected criteria. Please change your selection.")  # User-friendly message
        else:
            # Convert timestamps to datetime and extract hour for visualization
            data["start_time"] = pd.to_datetime(data["start_time"])
            data["end_time"] = pd.to_datetime(data["end_time"])
            data["start_hour"] = data["start_time"].dt.hour
            data["end_hour"] = data["end_time"].dt.hour

            # Filter Options
            selected_id = st.sidebar.selectbox("Filter by Job ID", options=["All"] + list(data["job_id"].unique()))
            selected_status = st.sidebar.selectbox("Filter by Status", options=["All"] + list(data["status"].unique()))
            time_range = st.sidebar.slider("Filter by Execution Time", min_value=0, max_value=int(data["execution_time"].max()), value=(0, int(data["execution_time"].max())))

            # Apply Filters
            filtered_data = data
            if selected_id != "All":
                filtered_data = filtered_data[filtered_data["job_id"] == selected_id]
            if selected_status != "All":
                filtered_data = filtered_data[filtered_data["status"] == selected_status]
            filtered_data = filtered_data[ 
                (filtered_data["execution_time"] >= time_range[0]) & 
                (filtered_data["execution_time"] <= time_range[1])
            ]

            # Export Data
            st.download_button(
                label="Download Data as CSV",
                data=filtered_data.to_csv(index=False),
                file_name="filtered_data.csv",
                mime="text/csv"
            )

            # Metrics
            st.markdown("### Metrics")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Records", len(filtered_data))
            col2.metric("Success Count", len(filtered_data[filtered_data["status"] == "Success"]))
            col3.metric("Failed Count", len(filtered_data[filtered_data["status"] == "FAILED"]))
            col4.metric("Avg Execution Time", round(filtered_data["execution_time"].mean(), 2))

            # Detailed Data
            st.markdown("### Filtered Data")
            st.dataframe(filtered_data.style.applymap(
                lambda x: "background-color: green; color: white;" if x == "Success" else (
                    "background-color: red; color: white;" if x == "FAILED" else "")
                , subset=["status"]
            ), use_container_width=True)

            # Execution Trend Visualization
            st.markdown("### Job Execution Trend (By Hour)")
            hour_data = filtered_data.groupby("start_hour").size().reset_index(name="count")
            bar_chart = alt.Chart(hour_data).mark_bar().encode(
                x=alt.X("start_hour:O", title="Hour of Day"),
                y=alt.Y("count:Q", title="Number of Jobs"),
                tooltip=["start_hour", "count"]
            ).properties(title="Jobs Executed by Hour", width=600, height=400)
            st.altair_chart(bar_chart, use_container_width=True)

            # Heatmap
            st.markdown("### Heatmap of Execution Times")
            heatmap_data = filtered_data.pivot_table(index="job_id", columns="status", values="execution_time", aggfunc="mean")
            st.dataframe(heatmap_data)

            # Old Heatmap
            st.markdown("### Heatmap of Execution Times")
            heatmap_data = filtered_data.pivot_table(index="business_date", columns="job_id", values="execution_time", aggfunc="mean").fillna(0)
            st.dataframe(heatmap_data)



with tab2:
    # Sample DataFrame for dependent jobs
    def get_dependent_jobs():
        """Create a DataFrame for dependent jobs."""
        dependent_jobs_data = {
            "job_id": ["001", "001", "001", "002", "003", "004", "005", "abc", "xyz"],
            "dependent_job_id": ["002", "004", None, None, "001", "005", None, None, "abc"],
        }
        return pd.DataFrame(dependent_jobs_data)

    # Function to wrap node labels into multiple lines if they exceed max_length
    def wrap_label(label, max_length=10):
        """Wrap labels to fit within the nodes by splitting long labels into multiple lines."""
        return "\n".join(textwrap.wrap(label, width=max_length))

    # Display Dependent Jobs Data
    st.markdown("### Dependent Jobs Data")
    dependent_jobs_df = get_dependent_jobs()

    # Sidebar Inputs for Job Dependency Graph
    st.sidebar.markdown("### Job Dependency Graph Inputs")
    show_all_jobs = st.sidebar.checkbox("Show All Jobs", value=True)
    
    # Instead of text_input, use multiselect for multiple job IDs
    specific_job_ids = []
    if not show_all_jobs:
        specific_job_ids = st.sidebar.multiselect(
            "Select Specific Job IDs", 
            options=dependent_jobs_df["job_id"].unique().tolist(),
            default=[]
        )

    # Form Inputs for Job Dependency Graph
    st.markdown("### Configure Job Dependency Graph")
    with st.form("dependency_graph_form"):
        highlight_dependencies = st.checkbox("Highlight Dependencies", value=True)
        max_depth = st.slider("Max Dependency Depth", min_value=1, max_value=5, value=3)
        layout_algo = st.selectbox("Select Graph Layout", options=["spring_layout", "circular_layout", "kamada_kawai_layout"])
        submitted = st.form_submit_button("Generate Graph")  # Submit button for graph generation

    # Apply filtering based on inputs
    if specific_job_ids:  # Check if specific job IDs are selected
        dependent_jobs_df = dependent_jobs_df[
            dependent_jobs_df["job_id"].isin(specific_job_ids) |
            dependent_jobs_df["dependent_job_id"].isin(specific_job_ids)
        ]

    st.dataframe(dependent_jobs_df, use_container_width=True)

    # Generate Dependency Graph if submitted
    if submitted:
        st.markdown("### Job Dependency Graph")
        G = nx.DiGraph()
        dependent_jobs = dependent_jobs_df.dropna(subset=["dependent_job_id"])  # Filter out rows with no dependencies

        # Add edges for valid job dependencies
        for _, row in dependent_jobs.iterrows():
            G.add_edge(row["job_id"], row["dependent_job_id"])

        # Create the plot
        fig, ax = plt.subplots(figsize=(10, 10))  # Size of the graph
        
        # Dynamically change the layout based on user input
        if layout_algo == "spring_layout":
			#pos = nx.spring_layout(G)
			#pos = nx.spring_layout(G, k=4, iterations=50, scale=2)  # Adjust 'k' for node distance
            pos = nx.spring_layout(G, k=1, iterations=50, scale=2)
        elif layout_algo == "circular_layout":
            pos = nx.circular_layout(G)
        elif layout_algo == "kamada_kawai_layout":
            pos = nx.kamada_kawai_layout(G)

        # Wrapping labels for readability
        wrapped_labels = {node: wrap_label(node) for node in G.nodes()}

        # Increase node size and adjust font size to fit wrapped labels
        nx.draw(
            G, pos,
            with_labels=True,
            labels=wrapped_labels,
            node_size=5000,  # Increased node size
            node_color="skyblue",
            font_size=8,  # Adjust font size for wrapped labels
            font_weight="bold",
            ax=ax
        )

        # Highlight dependencies if selected
        if highlight_dependencies:
            edges = list(G.edges)
            nx.draw_networkx_edges(
                G, pos, edgelist=edges,
                edge_color="red",
                width=2,
                arrowstyle="->",
                arrowsize=10,  # Increase this value to lengthen the arrows
                ax=ax
            )

        st.pyplot(fig)
