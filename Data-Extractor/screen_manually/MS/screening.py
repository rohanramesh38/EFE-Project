import functions
import numpy as np
import re
import pandas as pd
import os

keys = {"Total_Advisor_Led_AUM": "Advisor-led client assets", \
        "Fee_Based_AUM": "Fee-based client assets", \
            "Fee_Based_Flows": "Fee-based asset flows", \
                "Asset_Mgmt_Rev": "Asset management", \
                    "TotalNNA": "Net new assets"}

report_year = "4Q23"

quarters = functions.name_past_6Q(report_year) #eg. return ["2Q23", "1Q23", "4Q22", "3Q22", "2Q22", "1Q22", "4Q21"]
reports = [quarters[0], quarters[2], quarters[-2]] #only reports of 2Q23, 4Q22, 1Q22 is needed.

dict_page = {}
for report in reports:
    dict_page[report] = functions.extract_pageinfo_title(report, "Wealth Management") # extract the page's info(quarter, page, unit) of title with Wealth Management


selected_tables = functions.extract_tables_with_pagenum(dict_page) # extract tables from pages with the specified title

times = functions.explain_time(quarters) # explain 2Q23 as Jun 30, 2023

output_dict = {}

# extract the absolute value
columns_name=["Key"] + quarters
output_dict[columns_name[0]] = list(keys.keys())
for i in columns_name[1:]:
    datas = []
    time = times[i]
    for j in output_dict[columns_name[0]]:
        for quarter, page_number, unit, df in selected_tables:
            if any(df["key"].str.contains(functions.preprocess_string(keys[j]), na=False)) and time in df.columns:
                data = df.loc[df["key"].str.contains(functions.preprocess_string(keys[j]), na=False), time]
                data = data.to_frame().iloc[0, 0]
                data = float(re.sub(r'[$, ]', '', data))
                if unit[0] == "millions":
                    data /= 1000
        datas.append(data)
    output_dict[i] = datas

output = pd.DataFrame(output_dict)
output["QoQ%"] = 100*(output[report_year]-output[quarters[1]])/output[quarters[1]]
output["YoY%"] = 100*(output[report_year]-output[quarters[4]])/output[quarters[4]]

output[columns_name[1:-2]] = output[columns_name[1:-2]].round(1)
output["QoQ%"] = output["QoQ%"].astype(int)
output["YoY%"] = output["YoY%"].astype(int)

output.to_excel(f"./code/screen_template/MS/sample_result/{report_year}.xlsx")