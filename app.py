import streamlit as st
import os
import docx
import pandas as pd
import tempfile
import zipfile
from io import BytesIO
import hashlib
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Set page configuration
st.set_page_config(
    page_title="HSE Data Extractor",
    page_icon="📊",
    layout="wide"
)

# Database setup
Base = declarative_base()

class HSERecord(Base):
    __tablename__ = 'hse_records'
    
    id = Column(Integer, primary_key=True)
    filename = Column(String(255), nullable=False)
    date_extracted = Column(String(100))
    mereenie_hse = Column(Text)
    palm_valley_hse = Column(Text)
    becgs_dingo_hse = Column(Text)
    processed_date = Column(DateTime, default=datetime.now)
    file_size = Column(Integer)

@st.cache_resource
def init_database():
    """Initialize database connection and create tables"""
    try:
        engine = create_engine(os.environ['DATABASE_URL'])
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        return engine, Session
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None, None

def save_to_database(data, session_maker):
    """Save processed HSE data to database"""
    if session_maker is None:
        return False
    
    session = None
    try:
        session = session_maker()
        for record in data:
            hse_record = HSERecord(
                filename=record['File'],
                date_extracted=record['Date'],
                mereenie_hse=record['Mereenie_HSE'],
                palm_valley_hse=record['Palm Valley_HSE'],
                becgs_dingo_hse=record['BECGS/Dingo_HSE'],
                file_size=record.get('file_size', 0)
            )
            session.add(hse_record)
        session.commit()
        session.close()
        return True
    except Exception as e:
        if session:
            session.rollback()
            session.close()
        st.error(f"Database save failed: {str(e)}")
        return False

def get_database_records(session_maker, limit=100):
    """Retrieve recent HSE records from database"""
    if session_maker is None:
        return []
    
    try:
        session = session_maker()
        records = session.query(HSERecord).order_by(HSERecord.processed_date.desc()).limit(limit).all()
        session.close()
        return records
    except Exception as e:
        st.error(f"Database query failed: {str(e)}")
        return []

def check_password():
    """
    Returns True if the password entered by the user is correct.
    """
    def password_entered():
        """
        Checks whether a password entered by the user is correct.
        """
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😞 Password incorrect")
        return False
    else:
        # Password correct
        return True

def process_docx_file(file_content, filename):
    """
    Process a single .docx file and extract HSE data from specific fields
    """
    tmp_file_path = None
    try:
        # Create a temporary file to save the uploaded content
        with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_file:
            tmp_file.write(file_content)
            tmp_file_path = tmp_file.name
        
        # Open the document
        doc = docx.Document(tmp_file_path)
        
        # Extract date from filename
        date_part = filename.replace(".docx", "").strip()
        
        # Initialize HSE comments dictionary
        hse_comments = {"Mereenie": [], "Palm Valley": [], "BECGS/Dingo": []}
        in_hse_section = False
        
        # Process tables in the document
        for table in doc.tables:
            header_row = [cell.text.strip() for cell in table.rows[0].cells]
            field_indices = {
                "Mereenie": header_row.index("Mereenie") if "Mereenie" in header_row else -1,
                "Palm Valley": header_row.index("Palm Valley") if "Palm Valley" in header_row else -1,
                "BECGS/Dingo": header_row.index("BECGS/Dingo") if "BECGS/Dingo" in header_row else -1
            }
            
            for row in table.rows:
                cells = [cell.text.strip() for cell in row.cells]
                
                if not any(cells):
                    continue
                
                if "HSE" in cells:
                    in_hse_section = True
                    for field, idx in field_indices.items():
                        if idx >= 0 and idx < len(cells) and cells[idx]:
                            if cells[idx] == "Nil":
                                hse_comments[field] = ["Nil"]
                            elif not any(header in cells[idx] for header in ["HSE", "Production"]):
                                hse_comments[field].append(cells[idx])
                    continue
                
                if "Production" in cells:
                    in_hse_section = False
                    break
        
        # Format HSE data
        hse_data = {
            "Mereenie": "; ".join(hse_comments["Mereenie"]) if hse_comments["Mereenie"] and hse_comments["Mereenie"] != ["Nil"] else "Nil",
            "Palm Valley": "; ".join(hse_comments["Palm Valley"]) if hse_comments["Palm Valley"] and hse_comments["Palm Valley"] != ["Nil"] else "Nil",
            "BECGS/Dingo": "; ".join(hse_comments["BECGS/Dingo"]) if hse_comments["BECGS/Dingo"] and hse_comments["BECGS/Dingo"] != ["Nil"] else "Nil"
        }
        
        # Clean up temporary file
        os.unlink(tmp_file_path)
        
        return {
            "File": filename,
            "Date": date_part,
            "Mereenie_HSE": hse_data["Mereenie"],
            "Palm Valley_HSE": hse_data["Palm Valley"],
            "BECGS/Dingo_HSE": hse_data["BECGS/Dingo"]
        }
        
    except Exception as e:
        # Clean up temporary file if it exists
        if tmp_file_path:
            try:
                os.unlink(tmp_file_path)
            except:
                pass
        raise e

def create_excel_file(data):
    """
    Create an Excel file from the extracted data
    """
    df = pd.DataFrame(data)
    
    # Create Excel file in memory
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    return output.getvalue()

def main():
    # Check password first
    if not check_password():
        st.stop()
    
    # Initialize database
    engine, Session = init_database()
    
    st.title("📊 HSE Data Extractor")
    st.markdown("Upload Word documents (.docx) to extract HSE data.")
    
    # Add tabs for different views
    tab1, tab2 = st.tabs(["📁 Upload & Process", "📊 Database Records"])
    
    with tab2:
        st.subheader("Previous HSE Records")
        if Session:
            records = get_database_records(Session)
            if records:
                # Convert database records to dataframe for display
                db_data = []
                for record in records:
                    db_data.append({
                        "ID": record.id,
                        "Filename": record.filename,
                        "Date Extracted": record.date_extracted,
                        "Mereenie HSE": record.mereenie_hse,
                        "Palm Valley HSE": record.palm_valley_hse,
                        "BECGS/Dingo HSE": record.becgs_dingo_hse,
                        "Processed Date": record.processed_date.strftime("%Y-%m-%d %H:%M:%S") if record.processed_date else ""
                    })
                
                df_db = pd.DataFrame(db_data)
                st.dataframe(df_db, use_container_width=True, height=400)
                
                # Download all database records as Excel
                if st.button("📥 Download All Records as Excel"):
                    excel_data = create_excel_file(db_data)
                    st.download_button(
                        label="📥 Download Database Records",
                        data=excel_data,
                        file_name=f"HSE_Database_Records_{datetime.now().strftime('%Y%m%d')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                st.info("No records found in database.")
        else:
            st.error("Database connection not available.")
    
    with tab1:
        # File upload section
        uploaded_files = st.file_uploader(
            "Choose .docx files",
            type="docx",
            accept_multiple_files=True,
            help="Select one or more Word documents (.docx) containing HSE data"
        )
        
        if uploaded_files:
            st.success(f"✅ {len(uploaded_files)} file(s) uploaded successfully!")
            
            # Display uploaded files
            with st.expander("📁 Uploaded Files", expanded=False):
                for file in uploaded_files:
                    st.write(f"• {file.name} ({file.size:,} bytes)")
            
            # Process files button
if st.button("🔄 Process Files", type="primary"):
    # Initialize progress bar and status
    progress_bar = st.progress(0)
    status_text = st.empty()

    processed_data = []
    errors = []

    # Process each file
    for i, uploaded_file in enumerate(uploaded_files):
        try:
            status_text.text(f"Processing: {uploaded_file.name}")

            # Read file content
            file_content = uploaded_file.read()

            # Process the file
            result = process_docx_file(file_content, uploaded_file.name)
            processed_data.append(result)

            # Update progress
            progress_bar.progress((i + 1) / len(uploaded_files))

        except Exception as e:
            errors.append(f"Error processing {uploaded_file.name}: {str(e)}")

    # Clear progress indicators
    progress_bar.empty()
    status_text.empty()

    # Display errors if any
    if errors:
        st.error("⚠️ Some files could not be processed:")
        for error in errors:
            st.write(f"• {error}")

    # Display results if any files were processed successfully
    if processed_data:
        # Save to database
        if Session:
            # Add file size to processed data
            for i, data in enumerate(processed_data):
                data['file_size'] = uploaded_files[i].size

            if save_to_database(processed_data, Session):
                st.success(f"✅ Successfully processed and saved {len(processed_data)} file(s) to database")
            else:
                st.warning(f"✅ Successfully processed {len(processed_data)} file(s) but database save failed")
        else:
            st.success(f"✅ Successfully processed {len(processed_data)} file(s)")

        # Create DataFrame for display
        df = pd.DataFrame(processed_data)

        # Display summary statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Files", len(processed_data))
        with col2:
            mereenie_entries = sum(1 for row in processed_data if row['Mereenie_HSE'] != 'Nil')
            st.metric("Mereenie Entries", mereenie_entries)
        with col3:
            palm_valley_entries = sum(1 for row in processed_data if row['Palm Valley_HSE'] != 'Nil')
            st.metric("Palm Valley Entries", palm_valley_entries)
        with col4:
            becgs_entries = sum(1 for row in processed_data if row['BECGS/Dingo_HSE'] != 'Nil')
            st.metric("BECGS/Dingo Entries", becgs_entries)

        # Display data preview
        st.subheader("📋 Data Preview")
        st.dataframe(df, use_container_width=True, height=400)

        # Generate Excel file
        try:
            excel_data = create_excel_file(processed_data)

            # Download button
            st.download_button(
                label="📥 Download Excel File",
                data=excel_data,
                file_name="HSE_Summary_IndividualFields.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary"
            )

        except Exception as e:
            st.error(f"Error creating Excel file: {str(e)}")
    else:
        st.error("❌ No files were processed successfully. Please check your files and try again.")
                
                # Generate Excel file
                try:
                    excel_data = create_excel_file(processed_data)
                    
                    # Download button
                    st.download_button(
                        label="📥 Download Excel File",
                        data=excel_data,
                        file_name="HSE_Summary_IndividualFields.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                    
                except Exception as e:
                    st.error(f"Error creating Excel file: {str(e)}")
            
            else:
                st.error("❌ No files were processed successfully. Please check your files and try again.")
    
    else:
        # Instructions when no files are uploaded
        st.info("👆 Please upload one or more .docx files to get started.")
        
        with st.expander("ℹ️ How to use this application", expanded=False):
            st.markdown("""
            **Steps to extract HSE data:**
            
            1. **Upload Files**: Click "Browse files" and select one or more Word documents (.docx)
            2. **Process**: Click "Process Files" to extract HSE data from the documents
            3. **Review**: Check the data preview and summary statistics
            4. **Download**: Click "Download Excel File" to get your results
            
            **What this application does:**
            - Extracts HSE comments from tables in Word documents
            - Looks for data in three specific fields: Mereenie, Palm Valley, and BECGS/Dingo
            - Handles "Nil" entries appropriately
            - Exports results to an Excel file with individual columns for each field
            
            **File Requirements:**
            - Files must be in .docx format (Microsoft Word)
            - Documents should contain tables with HSE data
            - Table headers should include the field names: Mereenie, Palm Valley, BECGS/Dingo
            """)

if __name__ == "__main__":
    main()
