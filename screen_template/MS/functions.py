import tabula
import PyPDF2
import pandas as pd
import re
import requests
import io


# input "2Q23"
# output ["2Q23", "1Q23", "4Q22", "3Q22", "2Q22", "1Q22", "4Q21"]
def name_past_6Q(currentQ):
    quarters = [currentQ]
    q = int(currentQ[0])
    year = int(currentQ[2:4])
    for i in range(6):
        if q == 1:
            q = 4
            year -= 1
        else:
            q -= 1
        if year < 0:
            year = 100 + year
        time = f"{q}Q{year}"
        quarters.append(time)
    return quarters

# access to reports by url
def reports_access(quarter):
    ms_url = f"https://www.morganstanley.com/content/dam/msdotcom/en/about-us-ir/finsup{int(quarter[0])}q{2000+int(quarter[2:4])}/finsup{quarter[0]}q{2000+int(quarter[2:4])}.pdf"
    response = requests.get(ms_url)
    pdf_content = io.BytesIO(response.content)
    return pdf_content

# extract the page information with defined title
def extract_pageinfo_title(report, title):
    pageinfo = []
    pdf_content = reports_access(report)
    pdf_reader = PyPDF2.PdfFileReader(pdf_content)
    total_pages = pdf_reader.numPages
    for page_number in range(total_pages):
        page = pdf_reader.getPage(page_number)
        text = page.extractText()
        lines = text.split('\n')
        if lines:           
            header = lines[0].strip()
            unit_line = lines[2].strip()
        # Check if the title is in the text of the page
            if title.lower().replace('\xa0', ' ').strip() in header.lower().replace('\xa0', ' ').strip():
                
                if "millions" in unit_line:
                    unit = "millions"
                elif "billions" in unit_line:
                    unit = "billions"
                pageinfo.append([page_number + 1, unit])
    return pageinfo  


# remove the special characters and space
def preprocess_string(s):
    if isinstance(s, str):
        return re.sub(r'\W+', '', s).lower()
    else:
        return s
# according to the pageinfor extracted, using tabula extract the tables
def extract_tables_with_pagenum(dict_page):
    selected_tables = []
    for i in dict_page.keys():
        ms_url = f"https://www.morganstanley.com/content/dam/msdotcom/en/about-us-ir/finsup{int(i[0])}q{2000+int(i[2:4])}/finsup{i[0]}q{2000+int(i[2:4])}.pdf"
        page_num = [item[0] for item in dict_page[i]]
        area_coordinates = [120, 20, 500, 1400]
        for j in page_num:
            df_list = tabula.read_pdf(ms_url, pages=j, multiple_tables=True) 
            if len(df_list) == 1:
                df = df_list[0]
                df.rename(columns={df.columns[0]: "key"}, inplace=True)
                df["key"] = df["key"].apply(preprocess_string)
                selected_tables.append((i, j, [item[1] for item in dict_page[i] if item[0] == j],df))
            else:
                # Because the size are different, it will detect multiple tables in one tables and make a mess.
                # the coordinates have to adjust manually.
                df_list = tabula.read_pdf(ms_url, pages=j, multiple_tables=True, area=area_coordinates)
                for table_number, df in enumerate(df_list, start=0):
                    df.rename(columns={df.columns[0]: "key"}, inplace=True)
                    df["key"] = df["key"].apply(preprocess_string)
                    selected_tables.append((i, j, [item[1] for item in dict_page[i] if item[0] == j],df))

    return selected_tables

# change 2Q23 to Jun 30, 2023
def explain_time(quarters):
    quarter_date = {}
    for quarter in quarters:
        if quarter[0] == "1":
            date = "Mar 31"
        elif quarter[0] == "2":
            date = "Jun 30"
        elif quarter[0] == "3":
            date = "Sep 30"
        else:
            date = "Dec 31"
        quarter_date[quarter] = f"{date}, {int(quarter[2:4]) + 2000}"
    return quarter_date



