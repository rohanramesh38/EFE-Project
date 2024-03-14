import boto3

from py_markdown_table.markdown_table import markdown_table
from rich.console import Console
from rich.markdown import Markdown
import pandas as pd
console = Console()
from urllib.request import urlretrieve
import re
import json
import os

from collections import defaultdict

from fuzzywuzzy import fuzz

import requests
from bs4 import BeautifulSoup
import PyPDF2
import requests
from io import BytesIO
from lxml import etree
import wget
import numpy as np
from IPython.display import display,Image


import logging
from botocore.exceptions import ClientError
import fitz

from data_extractor import get_all_companys_data
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \ (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36','Accept-Language': 'en-US, en;q=0.5'}




def convert_pdf_to_text(pdf_path,selected_pages):
    
    pdf_document = fitz.open(pdf_path)
    text = ""
    for page_number in range(pdf_document.page_count):
        if(page_number in selected_pages):
            page = pdf_document[page_number]
            text += page.get_text()
    
    pdf_document.close()
    
    return text
def convert_pdf_to_text_pypdf(pdf_url,selected_pages):
    text = ""
    try:
        response = requests.get(pdf_url,headers=HEADERS)
        print(response)
        if response.status_code != 200:
            print("Failed to fetch the PDF from the URL.")
            return text
        
        pdf_content = BytesIO(response.content)
        
        pdf_reader = PyPDF2.PdfReader(pdf_content)
            
        for page_number in range(len(pdf_reader.pages)):
            if(page_number in selected_pages):
                page = pdf_reader.pages[page_number]
                text +=page.extract_text().lower()
    
    except:
        print()

       
    return text



logger = logging.getLogger(__name__)


class BedrockWrapper:

    def __init__(self, bedrock_client):
        self.bedrock_client = bedrock_client

    def list_foundation_models(self):
        try:
            response = self.bedrock_client.list_foundation_models()
            models = response["modelSummaries"]
            logger.info("Got %s foundation models.", len(models))
        except ClientError:
            logger.error("Couldn't list foundation models.")
            raise
        else:
            return models
        
def extract_pages_with_text(pdf_path, search_text_thresholds):
    matching_pages = []
    pdf_document = fitz.open(pdf_path)
    for page_number in range(pdf_document.page_count):
        page = pdf_document[page_number]
        page_text = str(page.get_text()).lower()

        highest_threshold = 0
        s=0
        for search_text, threshold in search_text_thresholds:
            s+=threshold
            if search_text.lower() in page_text:
                highest_threshold += 100
            else:
                similarity = fuzz.partial_ratio(search_text.lower(), page_text)
                if similarity >= threshold:
                    highest_threshold += max(threshold, similarity)
        

        if highest_threshold>s:
            matching_pages.append(page_number )

    return matching_pages

def extract_pages_with_text_pypdf(pdf_url, search_text_thresholds):
    matching_pages = []
    try:
        # Fetch the PDF content from the URL
        response = requests.get(pdf_url,headers=HEADERS,stream=True)
        print(response)
        if response.status_code != 200:
            print("Failed to fetch the PDF from the URL.")
            return matching_pages
        
        # Read the PDF content from the response
        pdf_content = BytesIO(response.content)
        
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfReader(pdf_content)
            
        # Iterate through each page
        for page_number in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_number]
            page_text = page.extract_text().lower()
            
            highest_threshold = 0
            s = 0
            for search_text, threshold in search_text_thresholds:
                s += threshold
                if search_text.lower() in page_text:
                    highest_threshold += 100
                else:
                    similarity = fuzz.partial_ratio(search_text.lower(), page_text)
                    if similarity >= threshold:
                        highest_threshold += max(threshold, similarity)
                    
            if highest_threshold > s:
                matching_pages.append(page_number )  # Page numbers start from 1 in PyPDF2
    except:
        print()
    
            
    return matching_pages


# def extract_pages_with_text(pdf_path, search_texts, threshold=80,skip=0):
#     matching_pages = []
#     page_text=''
#     pdf_document = fitz.open(pdf_path)
#     for page_number in range(pdf_document.page_count):
#         page = pdf_document[page_number]
#         page_text = str(page.get_text()).lower()
        
#         similarity = 0

#         for search_text in search_texts:
#             if(search_text.lower() in page_text):
#                 similarity+=100
#             else:
#                 similarity+=fuzz.partial_ratio(search_text.lower(), page_text)
#         value=len(search_texts)-skip

#         if similarity >= threshold*(len(search_texts)-skip):
#             matching_pages.append(page_number + 1)

#     return matching_pages

def get_sample_text():
    text='Goldman Sachs Reports  \nSecond Quarter 2023 Earnings Results\nThe Goldman Sachs Group, Inc. and Subsidiaries\nSegment Net Revenues (unaudited)\n$ in millions\nAdvisory\n645\n$                  \n818\n$                \n1,197\n$               \n(21) %\n(46) %\nEquity underwriting\n338\n                    \n255\n                  \n145\n                    \n33\n133\nDebt underwriting\n448\n                    \n506\n                  \n457\n                    \n(11)\n(2)\nInvestment banking fees\n1,431\n                 \n1,579\n               \n1,799\n                 \n(9)\n(20)\nFICC intermediation\n2,089\n                 \n3,280\n               \n2,921\n                 \n(36)\n(28)\nFICC financing\n622\n                    \n651\n                  \n721\n                    \n(4)\n(14)\nFICC\n2,711\n                 \n3,931\n               \n3,642\n                 \n(31)\n(26)\nEquities intermediation\n1,533\n                 \n1,741\n               \n1,767\n                 \n(12)\n(13)\nEquities financing\n1,433\n                 \n1,274\n               \n1,177\n                 \n12\n22\nEquities\n2,966\n                 \n3,015\n               \n2,944\n                 \n(2)\n1\nOther\n81\n                      \n(81)\n                   \n(43)\n                     \nN.M.\nN.M.\nNet revenues\n7,189\n                 \n8,444\n               \n8,342\n                 \n(15)\n(14)\nManagement and other fees\n2,354\n                 \n2,282\n               \n2,243\n                 \n3\n5\nIncentive fees\n25\n                      \n53\n                    \n185\n                    \n(53)\n(86)\nPrivate banking and lending\n874\n                    \n354\n                  \n538\n                    \n147\n62\nEquity investments\n(403)\n                   \n119\n                  \n(104)\n                   \nN.M.\nN.M.\nDebt investments\n197\n                    \n408\n                  \n317\n                    \n(52)\n(38)\nNet revenues\n3,047\n                 \n3,216\n               \n3,179\n                 \n(5)\n(4)\nPLATFORM SOLUTIONS\nConsumer platforms\n577\n                    \n490\n                  \n252\n                    \n18\n129\nTransaction banking and other\n82\n                      \n74\n                    \n91\n                      \n11\n(10)\nNet revenues\n659\n                    \n564\n                  \n343\n                    \n17\n92\nTotal net revenues\n10,895\n$             \n12,224\n$           \n11,864\n$             \n(11)\n(8)\n$ in millions\nAmericas\n6,801\n$               \n7,194\n$             \n6,980\n$               \nEMEA\n2,868\n                 \n3,584\n               \n3,429\n                 \nAsia\n1,226\n                 \n1,446\n               \n1,455\n                 \nTotal net revenues\n10,895\n$             \n12,224\n$           \n11,864\n$             \nAmericas\n63%\n59%\n59%\nEMEA\n26%\n29%\n29%\nAsia\n11%\n12%\n12%\nTotal \n100%\n100%\n100%\nTHREE MONTHS ENDED\nJUNE 30,\n2023\nMARCH 31,\n2023\nJUNE 30,\n2022\nASSET & WEALTH MANAGEMENT\nGLOBAL BANKING & MARKETS\nGeographic Net Revenues (unaudited)3\nTHREE MONTHS ENDED\nJUNE 30,\n2023\nMARCH 31,\n2023\nJUNE 30,\n2022\n% CHANGE FROM\nMARCH 31,\n2023\nJUNE 30,\n2022\n7\nGoldman Sachs Reports  \nSecond Quarter 2023 Earnings Results\nThe Goldman Sachs Group, Inc. and Subsidiaries\nAssets Under Supervision (unaudited)3,4\n$ in billions\nASSET CLASS\nAlternative investments\n267\n$                     \n268\n$                \n254\n$                     \nEquity\n627\n                       \n597\n                  \n552\n                       \nFixed income\n1,056\n                    \n1,047\n               \n1,007\n                    \nTotal long-term AUS\n1,950\n                    \n1,912\n               \n1,813\n                    \nLiquidity products\n764\n                       \n760\n                  \n682\n                       \nTotal AUS\n2,714\n$                  \n2,672\n$             \n2,495\n$                 \nBeginning balance\n2,672\n$                  \n2,547\n$             \n2,394\n$                 \nNet inflows / (outflows):\nAlternative investments\n(1)\n                          \n1\n                      \n4\n                           \nEquity\n(3)\n                          \n(2)\n                     \n1\n                           \nFixed income\n12\n                         \n9\n                      \n(3)\n                         \nTotal long-term AUS net inflows / (outflows)\n8\n                           \n8\n                      \n2\n                           \nLiquidity products\n4\n                           \n49\n                    \n(7)\n                         \nTotal AUS net inflows / (outflows)\n12\n                         \n57\n                    \n(5)\n                         \nAcquisitions / (dispositions)\n-\n                            \n-\n                       \n305\n                       \nNet market appreciation / (depreciation)\n30\n                         \n68\n                    \n(199)\n                     \nEnding balance\n2,714\n$                  \n2,672\n$             \n2,495\n$                 \nJUNE 30,\n2023\nMARCH 31,\n2023\nJUNE 30,\n2022\nAS OF\nJUNE 30,\n2023\nMARCH 31,\n2023\nJUNE 30,\n2022\nTHREE MONTHS ENDED\n12\n'
    return text

def get_runtime():

    logging.basicConfig(level=logging.INFO)
    bedrock_client = boto3.client(service_name="bedrock", region_name="us-east-1")

    wrapper = BedrockWrapper(bedrock_client)
    client = boto3.client('bedrock-runtime')

    return client

def generate_data(text_from_pages,prompt):
    data=f'\n\nHuman: I m going to give you a table text extracted from a pdf and asking you to analyze the following information and extract from the key data features \\ntranscript.\\nHere is the transcript:\\n<transcript>\\n {text_from_pages} \\n </transcript> \\nYour task is to analyze that transcript, {prompt} \\n\\nAssistant:' 

    return data

def generate_body(data):
    body = json.dumps({
    'prompt': data,
    "max_tokens_to_sample": 1000,
    "temperature": 0.1,
    "top_p": 0.1,
    })
    return body

def invoke_claude_model(bedrock_client,body):
    modelId = 'anthropic.claude-v2'
    accept = 'application/json'
    contentType = 'application/json'

    response = bedrock_client.invoke_model(body=body, modelId=modelId, accept=accept, contentType=contentType)

    response_body = json.loads(response.get('body').read())

    result=response_body.get('completion')
    fappend_csv(result)

    console.print(Markdown(result))


    


gs_search_words=['Asset','Earnings Results','AUS']
schwab_search_words = ['Total advice solutions','Total net new assets' ]

gs_search_words_with_threshold=[('Assets Under Supervision',50),('Earnings Results',80),('AUS',100),('Wealth Management',60)]
schwab_search_words_with_threshold = [('Total asset management',50),('Total client assets',80),('Net New Assets',30) ]
bofa_search_words_with_threshold = [('Wealth Management',70,),('Assets under management ',70),('Total assets',100) ]
mgs_search_words_with_threshold =[('Wealth Management Metrics',80),('Advisor‐led channel',60),('Financial Information',40),('Asset Management',50)]

prompt='From the given information, create a csv table with containing wealth management information about Wealth AUS, Wealth net flows, Wealth Management Fees, Net market appreciation / (depreciation)and Total Revenues, Total AUS net inflows'

gs_prompt='''From the given information, create a markdown table with containing wealth management information about Wealth AUS,
 Wealth net flows, Wealth Management Fees, Net market appreciation / (depreciation)and Total Revenues,
 Total AUS, dont give an text other than the table and in the markdown table use  'B' for billion and 'M' for million '''
schwab_prompt='From the given information, create a markdown table with containing Asset management information bout Total net new assets, Net growth in client assets, Total Advice Solutions'
bofa_prompt='''From the given information, create a markdown table with containing Global Wealth & Investment Management information, 
Assent under management, Net Cliet Flow, Total Wealth Advisors.  dont give an text other than the table and in the markdown table use  'B' for billion and 'M' for million'''

mgs_prompt='''From the given information, create a markdown table with containing wealth management information, get Total client assets, Net new assets 
Advisor‐led client assets, Fee‐based client assets and flows , Self‐directed assets. dont give an text other than the table and in the markdown table use  'B' for billion and 'M' for million'''


def result1():
    schw_q4_2023_path='./../Data/schw_q4_2023.pdf'
    gs_2Q23_path='./../Data/gs2Q23.pdf'
    bofa_2023_4path='./../Data/bofa_4Q23.pdf'
    mgs_2023_4path='./../Data/mgs_4Q23.pdf'

    matching_pages=extract_pages_with_text(bofa_2023_4path,bofa_search_words_with_threshold)
    text=convert_pdf_to_text(bofa_2023_4path,matching_pages)
    print(matching_pages,bofa_2023_4path)
    #invoke(text,bofa_prompt)

    print()

    matching_pages=extract_pages_with_text(gs_2Q23_path,gs_search_words_with_threshold)
    text=convert_pdf_to_text(gs_2Q23_path,matching_pages)
    print(matching_pages,gs_2Q23_path)
    #invoke(text,gs_prompt)
    
    print()

    matching_pages=extract_pages_with_text(schw_q4_2023_path,schwab_search_words_with_threshold)
    text=convert_pdf_to_text(schw_q4_2023_path,matching_pages)
    print(matching_pages,schw_q4_2023_path)
    #invoke(text,schwab_prompt) 

    print(text,schwab_prompt)   



def result2():
    schwab_url='https://content.schwab.com/web/retail/public/about-schwab/schw_q4_2023_earnings_release.pdf'
    gs_2Q23_url='https://www.goldmansachs.com/media-relations/press-releases/current/pdfs/2023-q2-results.pdf'
    bofa_4Q2023_url='https://d1io3yog0oux5.cloudfront.net/_e26f734f80404bf9f15939064dbef560/bankofamerica/db/806/9995/supplemental_information/The+Supplemental+Information_4Q23_ADA.pdf'

    matching_pages=extract_pages_with_text_pypdf(bofa_4Q2023_url,bofa_search_words_with_threshold)
    text=convert_pdf_to_text_pypdf(bofa_4Q2023_url,matching_pages)
    print(matching_pages,bofa_4Q2023_url)
    #invoke(text,bofa_prompt)

    matching_pages=extract_pages_with_text_pypdf(gs_2Q23_url,gs_search_words_with_threshold)
    text=convert_pdf_to_text_pypdf(gs_2Q23_url,matching_pages)
    print(matching_pages,gs_2Q23_url)
    #invoke(text,gs_prompt)

    matching_pages=extract_pages_with_text_pypdf(schwab_url,schwab_search_words_with_threshold)
    text=convert_pdf_to_text_pypdf(schwab_url,matching_pages)
    print(matching_pages,schwab_url)
    invoke(text,gs_prompt)

def parse_rmd_table(table_string):
    # Find all rows
    rows = table_string.strip().split('\n')
    # Remove leading and trailing '|' and split by '|'
    rows = [re.split(r'\s*\|\s*', row.strip('|')) for row in rows]
    # Extract header and data
    header = rows[0]
    data = rows[2:]
    return header, data

def fappend(str):
    file1 = open("output.csv", "a")
    file1.write(str)
    file1.close()
def fappend_csv(rmd_table_string):

    if(rmd_table_string.find('|')):
        rmd_table_string=rmd_table_string[rmd_table_string.find('|'):]

    header, data = parse_rmd_table(rmd_table_string)

    max_columns = max(len(header), max(len(row) for row in data))
    #print(header,data)
    #print(type(header),type(data))


    header = header+ [' '] * (max_columns - len(header))

    data = [row + [''] * (max_columns - len(row)) for row in data]



    data_frame = pd.DataFrame(data, columns=header)

    data_frame.to_csv('output.csv', mode='a', index=False)



def result3():
    all_urls=get_all_companys_data()
    current_year=2024

    bofa=all_urls['BOFA']
    mgs=all_urls['MGS']
    print(bofa)

    fappend(f"#Bank of America")

    

    for year in range(current_year,current_year-3,-1):
        if(year in bofa):
            for quarter in bofa[year]:
                if(quarter['isAvailabe']):
                    print(f"Year: {year}   Quarter: {quarter['Quarter']}")
                    fappend(f"\nYear: {year} , Quarter: {quarter['Quarter']}\n")
                    process(quarter['FinancialSuppliments'],bofa_search_words_with_threshold,bofa_prompt)

    fappend(f"\n#Morgan Stanley")

    for year in range(current_year,current_year-3,-1):
        if(year in mgs):
            for quarter in mgs[year]:
                if(quarter['isAvailabe']):
                    print(f"Year: {year}   Quarter: {quarter['Quarter']}",quarter)
                    fappend(f"\nYear: {year},   Quarter: {quarter['Quarter']}\n")
                    process(quarter['FinancialSuppliments'],mgs_search_words_with_threshold,mgs_prompt)





def process(url,thresh,prompt):
    #print(url)
    matching_pages=extract_pages_with_text_pypdf(url,thresh)
    text=convert_pdf_to_text_pypdf(url,matching_pages)
    #print(matching_pages,mgs_search_words_with_threshold)
    invoke(text,prompt)

    


def invoke(text,prompt):
    data=generate_data(text,prompt)
    body=generate_body(data)
    invoke_claude_model(bedrock_client,body)




    
if __name__ == "__main__":
    bedrock_client= get_runtime()
    text_from_pages=get_sample_text()


    data=generate_data(text_from_pages,prompt)
    
    body=generate_body(data)

    #invoke_claude_model(bedrock_client,body)

    #result2()
    #result2()
    result3()

    

    


    


   




    #print(get_all_companys_data())

    



    #--------------------------------------------



    # matching_pages=extract_pages_with_text(gs_gs2Q23_path,gs_search_words,20,1)

    # print(matching_pages)

    # text=convert_pdf_to_text(gs_gs2Q23_path,matching_pages)

    # data=generate_data(text,prompt)
    
    # body=generate_body(data)
    #print(text)

    #invoke_claude_model(bedrock_client,body)
    

    #--------------------------------------------
    '''
    matching_pages=extract_pages_with_text(schwab_url,schwab_search_words,60,0)

    #print(matching_pages)

    text=convert_pdf_to_text(schwab_url,matching_pages)
    #print(text)

    prompt='From the given information, create a markdown table with containing wealth management information about Wealth AUS, Wealth net flows, Wealth Management Fees, Net market appreciation / (depreciation)and Total Revenues,Total net new assets,Total advice solutions, Advice solutions'

    data=generate_data(text,prompt)
    
    body=generate_body(data)
    invoke_claude_model(bedrock_client,body)
    '''


    #--------------------------------------------


  
