import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import zipfile
import io
from io import StringIO
from datetime import datetime
import os
import tempfile

# Database imports
from sqlalchemy import create_engine, Column, Integer, String, Text, Float, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

# Configuration
st.set_page_config(
    page_title="GBB Data Manager",
    page_icon="⚡",
    layout="wide"
)

# Database setup
Base = declarative_base()

class GBBRecord(Base):
    __tablename__ = 'gbb_records'
    
    id = Column(Integer, primary_key=True)
    gas_date = Column(DateTime)
    facility_name = Column(String(255))
    facility_id = Column(Integer)
    facility_type = Column(String(50))
    demand = Column(Float)
    supply = Column(Float)
    transfer_in = Column(Float)
    transfer_out = Column(Float)
    held_in_storage = Column(Float)
    cushion_gas_storage = Column(Float)
    state = Column(String(10))
    location_name = Column(String(255))
    location_id = Column(Integer)
    last_updated = Column(DateTime)
    imported_date = Column(DateTime, default=func.now())

def get_database_connection():
    """Get database connection using environment variables"""
    try:
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            st.error("DATABASE_URL not found in environment variables")
            return None, None
        
        engine = create_engine(database_url)
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        return engine, Session
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None, None

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_gbb_data(data_period_months=3):
    """Fetch and parse the latest GBB data from nemweb.com.au"""
    url = "https://nemweb.com.au/Reports/Current/GBB/GasBBActualFlowStorage.zip"
    
    try:
        # Download the ZIP file with progress
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()
        
        # Get total size for progress tracking
        total_size = int(response.headers.get('content-length', 0))
        downloaded_data = b''
        
        # Download in chunks
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                downloaded_data += chunk
        
        # Extract CSV from ZIP
        with zipfile.ZipFile(io.BytesIO(downloaded_data)) as zip_file:
            # Get the first CSV file in the archive (check both .csv and .CSV)
            csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
            if not csv_files:
                st.error("No CSV files found in the ZIP archive")
                return None
            
            # Read the CSV content
            with zip_file.open(csv_files[0]) as csv_file:
                csv_content = csv_file.read().decode('utf-8')
                
                # Parse CSV with pandas
                df = pd.read_csv(StringIO(csv_content))
                
                # Filter based on user-defined period
                df['GasDate'] = pd.to_datetime(df['GasDate'])
                
                if data_period_months > 0:  # 0 means "all data"
                    cutoff_date = pd.Timestamp.now() - pd.DateOffset(months=data_period_months)
                    recent_df = df[df['GasDate'] >= cutoff_date].copy()
                else:
                    recent_df = df.copy()
                
                # Limit to key facilities for faster demo
                key_facilities = ['Mereenie', 'Palm Valley', 'Yellerr', 'Yelcherr', 'NGP']
                if not recent_df.empty:
                    # Keep all data but prioritize key facilities
                    key_data = recent_df[recent_df['FacilityName'].str.contains('|'.join(key_facilities), case=False, na=False)]
                    other_data = recent_df[~recent_df['FacilityName'].str.contains('|'.join(key_facilities), case=False, na=False)]
                    
                    # Take all key facility data + sample of others for performance
                    if len(other_data) > 15000:
                        other_data = other_data.sample(n=15000, random_state=42)
                    
                    recent_df = pd.concat([key_data, other_data])
                
                return recent_df
                
    except requests.exceptions.Timeout:
        st.error("Download timeout - the data file is large. Please try again.")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Network error: {str(e)}")
        return None
    except Exception as e:
        st.error(f"Failed to fetch GBB data: {str(e)}")
        return None

def import_gbb_data_to_db(session_maker, df):
    """Import GBB data to the database"""
    if session_maker is None or df is None:
        return False
    
    session = None
    try:
        session = session_maker()
        
        # Clear existing GBB data
        session.query(GBBRecord).delete()
        session.commit()
        
        # Use pandas to_sql for much faster bulk insert
        progress_placeholder = st.empty()
        total_rows = len(df)
        
        progress_placeholder.text(f"Preparing {total_rows:,} records for bulk import...")
        
        # Prepare DataFrame for direct SQL insert
        import_df = df.copy()
        
        # Rename columns to match database schema
        column_mapping = {
            'GasDate': 'gas_date',
            'FacilityName': 'facility_name',
            'FacilityId': 'facility_id',
            'FacilityType': 'facility_type',
            'Demand': 'demand',
            'Supply': 'supply',
            'TransferIn': 'transfer_in',
            'TransferOut': 'transfer_out',
            'HeldInStorage': 'held_in_storage',
            'CushionGasStorage': 'cushion_gas_storage',
            'State': 'state',
            'LocationName': 'location_name',
            'LocationId': 'location_id',
            'LastUpdated': 'last_updated'
        }
        
        # Rename columns that exist
        import_df = import_df.rename(columns={k: v for k, v in column_mapping.items() if k in import_df.columns})
        
        # Add imported_date column
        import_df['imported_date'] = datetime.now()
        
        # Convert dates
        if 'gas_date' in import_df.columns:
            import_df['gas_date'] = pd.to_datetime(import_df['gas_date'])
        if 'last_updated' in import_df.columns:
            import_df['last_updated'] = pd.to_datetime(import_df['last_updated'])
        
        # Ensure string columns are properly sized
        if 'facility_name' in import_df.columns:
            import_df['facility_name'] = import_df['facility_name'].astype(str).str[:255]
        if 'facility_type' in import_df.columns:
            import_df['facility_type'] = import_df['facility_type'].astype(str).str[:50]
        if 'state' in import_df.columns:
            import_df['state'] = import_df['state'].astype(str).str[:10]
        if 'location_name' in import_df.columns:
            import_df['location_name'] = import_df['location_name'].astype(str).str[:255]
        
        progress_placeholder.text("Performing bulk insert to database...")
        
        # Use SQLAlchemy engine for bulk insert
        engine = session.get_bind()
        import_df.to_sql('gbb_records', engine, if_exists='append', index=False, method='multi', chunksize=5000)
        
        progress_placeholder.empty()
        session.close()
        return True
        
    except Exception as e:
        if session:
            session.rollback()
            session.close()
        st.error(f"Failed to import GBB data: {str(e)}")
        return False

def get_gbb_records(session_maker):
    """Get all GBB records from the database"""
    if session_maker is None:
        return []
    
    session = None
    try:
        session = session_maker()
        records = session.query(GBBRecord).all()
        session.close()
        return records
    except Exception as e:
        if session:
            session.close()
        st.error(f"Failed to retrieve GBB records: {str(e)}")
        return []

def main():
    st.title("⚡ Australian Energy Market - GBB Data Manager")
    st.markdown("Real-time Gas Bulletin Board (GBB) data analysis from [nemweb.com.au](https://nemweb.com.au)")
    
    # Initialize database connection
    engine, Session = get_database_connection()
    
    if engine is None or Session is None:
        st.error("Failed to connect to database. Please check your configuration.")
        return
    
    # Data import section
    st.subheader("📊 Data Import Configuration")
    
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.write("**Import Latest Data**")
        st.info("Data is automatically cached for 1 hour to reduce server load")
    
    with col2:
        # Data period selection
        data_period_options = {
            "3 Months": 3,
            "6 Months": 6, 
            "1 Year": 12,
            "All Data": 0
        }
        
        selected_period = st.selectbox(
            "Data Period",
            list(data_period_options.keys()),
            index=0,
            help="Choose how much historical data to load"
        )
        
        data_months = data_period_options[selected_period]
    
    with col3:
        if st.button("🔄 Fetch Latest GBB Data", type="primary"):
            with st.spinner(f"Downloading and processing GBB data ({selected_period})..."):
                gbb_df = fetch_gbb_data(data_months)
                
                if gbb_df is not None:
                    # Show preview of raw data
                    st.success(f"✅ Successfully fetched {len(gbb_df)} records")
                    
                    # Import to database
                    if import_gbb_data_to_db(Session, gbb_df):
                        st.success("✅ Data imported to database successfully")
                    else:
                        st.warning("⚠️ Data fetched but database import failed")
                    
                    # Show data preview
                    st.subheader("📋 Raw Data Preview")
                    st.dataframe(gbb_df.head(10), use_container_width=True)
    
    st.divider()
    
    # Display stored GBB data
    st.subheader("🗄️ Stored GBB Data")
    gbb_records = get_gbb_records(Session)
    
    if gbb_records:
        # Convert to DataFrame for better display
        gbb_data = []
        for record in gbb_records:
            gbb_data.append({
                "Gas Date": record.gas_date.strftime("%Y-%m-%d") if record.gas_date else "N/A",
                "Facility Name": record.facility_name,
                "Facility ID": record.facility_id,
                "Facility Type": record.facility_type,
                "Demand": record.demand,
                "Supply": record.supply,
                "Transfer In": record.transfer_in,
                "Transfer Out": record.transfer_out,
                "Storage": record.held_in_storage,
                "State": record.state,
                "Location": record.location_name,
                "Last Updated": record.last_updated.strftime("%Y-%m-%d %H:%M") if record.last_updated else "N/A",
                "Imported": record.imported_date.strftime("%Y-%m-%d %H:%M")
            })
        
        gbb_df_display = pd.DataFrame(gbb_data)
        
        # Summary statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Records", len(gbb_records))
        with col2:
            facilities = len(set([r.facility_name for r in gbb_records if r.facility_name]))
            st.metric("Unique Facilities", facilities)
        with col3:
            total_supply = sum([r.supply for r in gbb_records if r.supply])
            st.metric("Total Supply", f"{total_supply:.2f}")
        with col4:
            latest_import = max([r.imported_date for r in gbb_records])
            st.metric("Last Updated", latest_import.strftime("%Y-%m-%d %H:%M"))
        
        # Data table with filtering
        st.subheader("📋 GBB Records")
        
        # Filter options
        col1, col2 = st.columns(2)
        
        with col1:
            # Filter by facility type
            facility_types = sorted(list(set([r.facility_type for r in gbb_records if r.facility_type])))
            if facility_types:
                selected_types = st.multiselect(
                    "Filter by Facility Type",
                    facility_types,
                    default=facility_types
                )
            else:
                selected_types = []
        
        with col2:
            # Filter by state
            states = sorted(list(set([r.state for r in gbb_records if r.state])))
            if states:
                selected_states = st.multiselect(
                    "Filter by State",
                    states,
                    default=states
                )
            else:
                selected_states = []
        
        # Show all data if no filters applied or if filters are applied
        if not selected_types:
            selected_types = facility_types
        if not selected_states:
            selected_states = states
            
        filtered_data = [row for row in gbb_data 
                       if (not facility_types or row['Facility Type'] in selected_types) 
                       and (not states or row['State'] in selected_states)]
        filtered_df = pd.DataFrame(filtered_data)
        st.dataframe(filtered_df, use_container_width=True, height=400)
        
        # GBB Charts - Always show if we have data
        if len(filtered_df) > 0:
            st.subheader("📊 GBB Data Visualization")
            
            chart_type = st.selectbox(
                "Select Chart Type",
                ["Specific Facilities Over Time", "Supply vs Demand", "Facility Type Comparison", "State Analysis", "Time Series", "Storage Analysis"]
            )
            
            if chart_type == "Specific Facilities Over Time":
                st.write("**Custom Facility Analysis**")
                st.info("Track supply trends for specific facilities over time with customizable toggles")
                
                # Convert gas date for time series
                time_data = filtered_df.copy()
                time_data['Gas Date'] = pd.to_datetime(time_data['Gas Date'])
                
                # Get available facilities
                all_facilities = sorted(time_data['Facility Name'].unique())
                
                # Check for your requested facilities and add them as defaults
                target_facilities = ['Yellerr', 'Mereenie', 'Palm Valley', 'Yelcherr', 'NGP']
                available_targets = [f for f in target_facilities if f in all_facilities]
                
                if not available_targets:
                    # Look for partial matches
                    for target in target_facilities:
                        matches = [f for f in all_facilities if target.lower() in f.lower()]
                        available_targets.extend(matches)
                
                # Default to target facilities if available, otherwise show top 5
                default_facilities = available_targets if available_targets else all_facilities[:5]
                
                # Facility selection with toggles
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    selected_facilities = st.multiselect(
                        "Select Facilities to Display",
                        all_facilities,
                        default=default_facilities,
                        help="Choose specific facilities to compare their supply trends over time"
                    )
                
                with col2:
                    # Quick toggle buttons for target facilities
                    st.write("**Quick Select:**")
                    for facility in target_facilities:
                        matching_facilities = [f for f in all_facilities if facility.lower() in f.lower()]
                        if matching_facilities:
                            facility_name = matching_facilities[0]
                            if st.button(f"Toggle {facility}", key=f"toggle_{facility}"):
                                if facility_name in selected_facilities:
                                    selected_facilities.remove(facility_name)
                                else:
                                    selected_facilities.append(facility_name)
                                st.rerun()
                
                if selected_facilities:
                    # Filter data for selected facilities
                    facility_data = time_data[time_data['Facility Name'].isin(selected_facilities)].copy()
                    
                    # Group by date and facility for time series
                    daily_facility_data = facility_data.groupby(['Gas Date', 'Facility Name']).agg({
                        'Supply': 'sum',
                        'Demand': 'sum',
                        'Transfer In': 'sum',
                        'Transfer Out': 'sum'
                    }).reset_index()
                    
                    # Chart options
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        metric_to_plot = st.selectbox(
                            "Select Metric",
                            ["Supply", "Demand", "Transfer In", "Transfer Out"],
                            index=0
                        )
                    
                    with col2:
                        show_individual = st.checkbox("Show Individual Lines", value=True)
                        show_total = st.checkbox("Show Combined Total", value=False)
                    
                    with col3:
                        auto_y_range = st.checkbox("Auto Y-axis Range", value=True)
                    
                    # Y-axis range controls
                    if not auto_y_range:
                        st.write("**Y-axis Range Settings:**")
                        y_col1, y_col2 = st.columns(2)
                        
                        # Get data range for reference
                        all_values = daily_facility_data[metric_to_plot].dropna()
                        if len(all_values) > 0:
                            data_min = float(all_values.min())
                            data_max = float(all_values.max())
                            data_range = data_max - data_min
                            
                            with y_col1:
                                y_min = st.number_input(
                                    "Y-axis Minimum", 
                                    value=float(max(0, data_min - data_range * 0.1)),
                                    step=1.0,
                                    format="%.2f"
                                )
                            
                            with y_col2:
                                y_max = st.number_input(
                                    "Y-axis Maximum", 
                                    value=float(data_max + data_range * 0.1),
                                    step=1.0,
                                    format="%.2f"
                                )
                        else:
                            y_min, y_max = 0, 100
                    
                    # Separate facilities into two groups: NGP and Others
                    ngp_facilities = [f for f in selected_facilities if 'NGP' in f.upper()]
                    other_facilities = [f for f in selected_facilities if 'NGP' not in f.upper()]
                    
                    colors = px.colors.qualitative.Set1
                    
                    # Chart 1: Mereenie, Palm Valley, Yelcherr (non-NGP facilities)
                    if other_facilities:
                        st.subheader(f"📊 {metric_to_plot} - Mereenie, Palm Valley & Yelcherr")
                        fig1 = go.Figure()
                        
                        if show_individual:
                            for i, facility in enumerate(other_facilities):
                                facility_subset = daily_facility_data[daily_facility_data['Facility Name'] == facility]
                                if not facility_subset.empty:
                                    fig1.add_trace(
                                        go.Scatter(
                                            x=facility_subset['Gas Date'],
                                            y=facility_subset[metric_to_plot],
                                            name=facility,
                                            mode='lines',
                                            line=dict(color=colors[i % len(colors)], width=2)
                                        )
                                    )
                        
                        if show_total and len(other_facilities) > 1:
                            # Calculate total for non-NGP facilities
                            other_data = daily_facility_data[daily_facility_data['Facility Name'].isin(other_facilities)]
                            total_other = other_data.groupby('Gas Date')[metric_to_plot].sum().reset_index()
                            fig1.add_trace(
                                go.Scatter(
                                    x=total_other['Gas Date'],
                                    y=total_other[metric_to_plot],
                                    name=f'Total {metric_to_plot}',
                                    mode='lines',
                                    line=dict(color='black', width=3, dash='dash')
                                )
                            )
                        
                        # Update layout for chart 1
                        y_axis_config = dict(fixedrange=False)
                        if not auto_y_range and 'y_min' in locals() and 'y_max' in locals():
                            y_axis_config['range'] = [y_min, y_max]
                        
                        fig1.update_layout(
                            title=f'{metric_to_plot} Over Time - Mereenie, Palm Valley & Yelcherr',
                            xaxis_title='Date',
                            yaxis_title=f'{metric_to_plot} (TJ/day)',
                            height=500,
                            hovermode='x unified',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            ),
                            xaxis=dict(
                                rangeslider=dict(visible=True),
                                type="date"
                            ),
                            yaxis=y_axis_config
                        )
                        
                        # Add grid and styling
                        fig1.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                        fig1.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                        
                        st.plotly_chart(fig1, use_container_width=True)
                    
                    # Chart 2: NGP Transfer In (separate chart)
                    if ngp_facilities:
                        st.subheader("🔄 NGP Transfer In Analysis")
                        
                        # NGP Y-axis range controls
                        ngp_auto_y = st.checkbox("Auto Y-axis Range (NGP)", value=True, key="ngp_auto_y")
                        
                        if not ngp_auto_y:
                            st.write("**NGP Y-axis Range Settings:**")
                            ngp_col1, ngp_col2 = st.columns(2)
                            
                            # Get NGP Transfer In data range
                            ngp_data = daily_facility_data[daily_facility_data['Facility Name'].isin(ngp_facilities)]
                            ngp_values = ngp_data['Transfer In'].dropna()
                            
                            if len(ngp_values) > 0:
                                ngp_min = float(ngp_values.min())
                                ngp_max = float(ngp_values.max())
                                ngp_range = ngp_max - ngp_min
                                
                                with ngp_col1:
                                    ngp_y_min = st.number_input(
                                        "NGP Y-axis Minimum", 
                                        value=float(max(0, ngp_min - ngp_range * 0.1)),
                                        step=1.0,
                                        format="%.2f",
                                        key="ngp_y_min"
                                    )
                                
                                with ngp_col2:
                                    ngp_y_max = st.number_input(
                                        "NGP Y-axis Maximum", 
                                        value=float(ngp_max + ngp_range * 0.1),
                                        step=1.0,
                                        format="%.2f",
                                        key="ngp_y_max"
                                    )
                            else:
                                ngp_y_min, ngp_y_max = 0, 100
                        
                        fig2 = go.Figure()
                        
                        if show_individual:
                            for i, facility in enumerate(ngp_facilities):
                                facility_subset = daily_facility_data[daily_facility_data['Facility Name'] == facility]
                                if not facility_subset.empty:
                                    fig2.add_trace(
                                        go.Scatter(
                                            x=facility_subset['Gas Date'],
                                            y=facility_subset['Transfer In'],  # Always show Transfer In for NGP
                                            name=f"{facility}",
                                            mode='lines',
                                            line=dict(color='orange', width=3)
                                        )
                                    )
                        
                        # Update layout for NGP chart
                        ngp_y_axis_config = dict(fixedrange=False)
                        if not ngp_auto_y and 'ngp_y_min' in locals() and 'ngp_y_max' in locals():
                            ngp_y_axis_config['range'] = [ngp_y_min, ngp_y_max]
                        
                        fig2.update_layout(
                            title='NGP Transfer In Over Time',
                            xaxis_title='Date',
                            yaxis_title='Transfer In (TJ/day)',
                            height=500,
                            hovermode='x unified',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            ),
                            xaxis=dict(
                                rangeslider=dict(visible=True),
                                type="date"
                            ),
                            yaxis=ngp_y_axis_config
                        )
                        
                        # Add grid and styling
                        fig2.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                        fig2.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
                        
                        st.plotly_chart(fig2, use_container_width=True)
                    
                    # Show summary statistics
                    st.subheader("📈 Facility Summary Statistics")
                    summary_stats = []
                    for facility in selected_facilities:
                        facility_subset = daily_facility_data[daily_facility_data['Facility Name'] == facility]
                        if not facility_subset.empty:
                            stats = {
                                'Facility': facility,
                                f'Avg {metric_to_plot}': f"{facility_subset[metric_to_plot].mean():.2f}",
                                f'Max {metric_to_plot}': f"{facility_subset[metric_to_plot].max():.2f}",
                                f'Min {metric_to_plot}': f"{facility_subset[metric_to_plot].min():.2f}",
                                'Days of Data': len(facility_subset)
                            }
                            summary_stats.append(stats)
                    
                    if summary_stats:
                        st.dataframe(pd.DataFrame(summary_stats), use_container_width=True)
                else:
                    st.warning("Please select at least one facility to display the chart.")
            
            elif chart_type == "Supply vs Demand":
                fig = px.scatter(
                    filtered_df,
                    x='Supply',
                    y='Demand',
                    color='Facility Type',
                    title='Supply vs Demand by Facility Type',
                    hover_data=['Facility Name', 'State']
                )
                # Add diagonal line for balance
                max_val = max(filtered_df['Supply'].max(), filtered_df['Demand'].max())
                fig.add_trace(go.Scatter(x=[0, max_val], y=[0, max_val], mode='lines', name='Supply = Demand', line=dict(dash='dash')))
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)
            
            # Export functionality
            st.subheader("📥 Export GBB Data")
            csv_data = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download GBB Data as CSV",
                data=csv_data,
                file_name=f"gbb_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        else:
            st.info("No data available after filtering.")
    
    else:
        st.info("No GBB data available. Click 'Fetch Latest GBB Data' to import data.")
        st.markdown("**About GBB Data:**")
        st.markdown("""
        The Gas Bulletin Board (GBB) provides real-time information about:
        - Gas facility capacity and actual flows
        - Scheduled vs actual quantities
        - Settlement dates and facility details
        - Australian energy market data
        """)

if __name__ == "__main__":
    main()
