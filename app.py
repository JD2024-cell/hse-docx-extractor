import streamlit as st
import os
import docx
import pandas as pd
import tempfile
from io import BytesIO

st.set_page_config(page_title="HSE Data Extractor", page_icon="📊", layout="wide")

def check_password():
    def password_entered():
        if st.session_state["password"] == "HSE2024!":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.text_input("Password", type="password", on_change=password_entered, key="password")
        st.error("😞 Password incorrect")
        return False
    else:
        return True

def process_docx_file(file_content, filename):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.docx') as tmp_file:
        tmp_file.write(file_content)
        tmp_file_path = tmp_file.name

    doc = docx.Document(tmp_file_path)
    os.unlink(tmp_file_path)

    date_part = filename.replace(".docx", "").strip()
    hse_comments = {"Mereenie": [], "Palm Valley": [], "BECGS/Dingo": []}

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
                for field, idx in field_indices.items():
                    if idx >= 0 and idx < len(cells) and cells[idx]:
                        if cells[idx] == "Nil":
                            hse_comments[field] = ["Nil"]
                        elif not any(header in cells[idx] for header in ["HSE", "Production"]):
                            hse_comments[field].append(cells[idx])
                continue
            if "Production" in cells:
                break

    hse_data = {
        "Mereenie": "; ".join(hse_comments["Mereenie"]) if hse_comments["Mereenie"] and hse_comments["Mereenie"] != ["Nil"] else "Nil",
        "Palm Valley": "; ".join(hse_comments["Palm Valley"]) if hse_comments["Palm Valley"] and hse_comments["Palm Valley"] != ["Nil"] else "Nil",
        "BECGS/Dingo": "; ".join(hse_comments["BECGS/Dingo"]) if hse_comments["BECGS/Dingo"] and hse_comments["BECGS/Dingo"] != ["Nil"] else "Nil"
    }

    return {
        "File": filename,
        "Date": date_part,
        "Mereenie_HSE": hse_data["Mereenie"],
        "Palm Valley_HSE": hse_data["Palm Valley"],
        "BECGS/Dingo_HSE": hse_data["BECGS/Dingo"]
    }

def create_excel_file(data):
    df = pd.DataFrame(data)
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)
    return output.getvalue()

def main():
    if not check_password():
        st.stop()

    st.title("📊 HSE Data Extractor")
    st.markdown("Upload Word documents (.docx) to extract HSE data.")

    uploaded_files = st.file_uploader("Choose .docx files", type="docx", accept_multiple_files=True)

    if uploaded_files:
        if st.button("🔄 Process Files"):
            processed_data = []
            for uploaded_file in uploaded_files:
                file_content = uploaded_file.read()
                result = process_docx_file(file_content, uploaded_file.name)
                processed_data.append(result)

            if processed_data:
                df = pd.DataFrame(processed_data)
                st.dataframe(df)
                excel_data = create_excel_file(processed_data)
                st.download_button("📥 Download Excel File", data=excel_data,
                                   file_name="HSE_Summary_IndividualFields.xlsx",
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.warning("No data extracted.")
    else:
        st.info("Upload one or more .docx files to begin.")

if __name__ == "__main__":
    main()
