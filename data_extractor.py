
import requests
from bs4 import BeautifulSoup
from lxml import etree
import io
import os
import re
import numpy as np
from fuzzywuzzy import fuzz
import logging
import PyPDF2
import requests
from io import BytesIO
from collections import defaultdict 
import nest_asyncio
nest_asyncio.apply()
import json
from llama_parse import LlamaParse

HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \ (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36','Accept-Language': 'en-US, en;q=0.5'}

# Configuring logger
logger = logging.getLogger(__name__)
logging.basicConfig(filename='log_file.log', encoding='utf-8', level=logging.INFO)

key=os.getenv('llama_parse_key')
# Initializing LlamaParse object

parser =None

try:
    parser=LlamaParse(
    api_key='llx-dpXpRsFYAmpeblGlY4yNgGpA1B0jH2ztCuWJqSSRE8FQydWj',  
    result_type="markdown", 
    num_workers=4, 
    verbose=True,
    language="en" 
  )
except:
   print("")



# Function to get DOM object from URL
def get_dom_object(url,HEADERS):
  response = requests.get(url,headers=HEADERS)
  soup = BeautifulSoup(response.content, "html.parser")
  dom = etree.HTML(str(soup))
  logger.info(str(soup))
  return dom,(response.status_code == 200)


# Function to get JSON object from URL
def get_json_object(url,HEADERS):
  response = requests.get(url,headers=HEADERS)
  json_obj=json.loads(response.content)
  return json_obj,(response.status_code == 200)

# This function extracts specific data from an HTML document (DOM) using XPath queries
def get_data_with_xpath(dom,xpath,company_code,base_url):
  part=None
  report_url=""
  is_sumplimental=False

  if(company_code=="MGS"):
    element_list=dom.xpath(xpath)
    
    if(element_list):
      element=element_list[0]
      part=element.attrib["href"]
      report_url=base_url+part
      return report_url,(part!=None)

  if(company_code=="BOFA"):
    element_list=dom.xpath(xpath)

    if(element_list):
      element=element_list[0]
      report_url=element.attrib["href"]
      text='random'
      try:
        text=element.attrib["aria-label"]
      except:
        text='random'

      is_sumplimental =True if fuzz.partial_ratio(text.lower(),"supplemental information")>=90 else False

      return report_url,is_sumplimental


def get_company_data(dom,comany,current_year,is_json_req=False):

  data=defaultdict(lambda: []) 
  if(comany["Code"]=="MGS"):
    for year in range(comany["report_avalaible_from"],comany["report_avalaible_till"]-1,-1):
        data[year]=[]
        for quater in range(2,6):
          obj={}
          obj["Quarter"]=quater-1
          obj["FinancialSuppliments"]=""
          obj["isAvailabe"]=False
          index=current_year-year
          xpath_morgan_stanley=f"//html//body//div[1]//div[2]//div//div//div//section//article//table//tbody//tr[{index}]//td[{quater}]//a[2]"
          #print(xpath_morgan_stanley)

          report_url,is_available=get_data_with_xpath(dom,xpath_morgan_stanley,comany["Code"],comany["base_url"])
          if(is_available):
            obj["FinancialSuppliments"]=report_url
            obj["isAvailabe"]=True
            data[year].append(obj)

  if(comany["Code"]=="BOFA"):
    xpath_bofa=f"//*[@id='main']/div/div"
    element_list=dom.xpath(xpath_bofa)
    id_list=[ele.attrib["id"] for ele in element_list]
    for id in id_list:
      year=id.split("-")[0]
      data[int(year)]=[]
      for quater in range(1,5):
        for order in range(3,6):
          obj={}
          obj["Quarter"]=4-quater+1
          obj["FinancialSuppliments"]=""
          obj["isAvailabe"]=False
          xpath_bofa_anchor=f"//*[@id='{id}-box']/div/div[{quater}]/div/div/div[2]/div[{order}]/div/a"
          report_url,is_available=get_data_with_xpath(dom,xpath_bofa_anchor,comany["Code"],comany["base_url"])
          if(is_available):
              obj["FinancialSuppliments"]=report_url
              obj["isAvailabe"]=True
              data[int(year)].append(obj)
              break
          
  if(comany["Code"]=="JPM"):
    if(is_json_req):
      obj={}
      for value in dom["items"]:
        year=value['year'].strip()
        
        if(obj):
          data[int(year)].append(obj)
        
        obj={}  

        quarter=value['quarter'][0]
        obj["Quarter"]=quarter
        obj["FinancialSuppliments"]=""
        obj["isAvailabe"]=False

        try:
          link=value["docs"]["supplement"]["link"]
          obj["FinancialSuppliments"]=comany["base_url"]+link
          obj["isAvailabe"]=True

        except:
          logger.info("Error")

  if(comany["Code"]=="GS"):

    path_gs=f"//*[@id='blogArticles']/div/div"
    url_list=[]

    element_list=dom.xpath(path_gs)

    for ele in element_list:
       for acordian in ele[2].iter():
          for link in acordian.iter():
            if("href" in link.attrib):
              url=link.attrib["href"]
              if(url not in url_list):
                 url_list.append(comany["base_url"]+url)
    final_all_urls=set()

    for url in url_list:
      dom,is_success=get_dom_object(url,HEADERS)
      pdf_url=''

      results_links = dom.xpath("//a[contains(text(), 'Earnings Results')]")
      results_links1 = dom.xpath("//a[contains(text(), 'Results')]")
      results_links2=dom.xpath("//a[contains(text(),'Quarter Results')]")
      results_links3=dom.xpath("//a[contains(text(),'Quarter Earnings')]")
      results_links4 = dom.xpath("//a[contains(., 'Earnings Results')]")


      all_list=[results_links3,results_links2,results_links1,results_links,results_links4]

      for list_item in all_list:
        if(list_item and "href" in list_item[0].attrib):
          final_url=''
          part=list_item[0].attrib["href"].strip()

          if("goldmansachs.com" not in part):
             final_url=comany["base_url"]+part
          else:
             final_url=part
          if("current" in final_url   ):
            pattern = r'(\d{4}-q\d{1})'
            matches = re.findall(pattern, final_url)

            if matches:
              for match in matches:
                  year, quarter = match.split("-q")
                  obj={}
                  obj["Quarter"]=quarter
                  obj["FinancialSuppliments"]=final_url
                  obj["isAvailabe"]=True
                  if(final_url not in final_all_urls):
                     data[int(year)].append(obj)
                     final_all_urls.add(final_url)
                     break

          break
           

  return data


company_list=[
             
              {  "Name":"Bank of America",
                 "Code":"BOFA",
                 "report_avalaible_from":2018,
                 "report_avalaible_till":2023,
                 "url":"https://investor.bankofamerica.com/quarterly-earnings",
                 "base_url":"https://investor.bankofamerica.com/quarterly-earnings",
                 "json_req":False
             },
             {   "Name":"Morgan Stanley",
                 "Code":"MGS",
                 "report_avalaible_from":2023,
                 "report_avalaible_till":2002,
                 "url":"https://www.morganstanley.com/about-us-ir/earnings-releases",
                 "base_url":"https://www.morganstanley.com",
                 "json_req":False
             },
             {  "Name":"JP Morgan",
                 "Code":"JPM",
                 "report_avalaible_from":2007,
                 "report_avalaible_till":2023,
                 "url":"https://www.jpmorganchase.com/ir/quarterly-earnings",
                 "base_url":"https://www.jpmorganchase.com/",
                 "json_url":"https://www.jpmorganchase.com/services/json/jpmc/ir-service/path.service/type=quarterly-earnings.json",
                 "json_req":True
             },{
                "Name":"Goldman Sachs",
                "Code":"GS",
                 "report_avalaible_from":2010,
                 "report_avalaible_till":2023,
                 "url":"https://www.goldmansachs.com/investor-relations/financials/quarterly-earnings-releases/index.html",
                 "base_url":"https://www.goldmansachs.com/",
                 "json_req":False

             },{
                "Name":"Schwab",
                "Code":"SCHWB",
                 "report_avalaible_from":2023,
                 "report_avalaible_till":2023,
                 "url":"https://www.aboutschwab.com/financial-reports",
                 "base_url":"https://content.schwab.com/web/retail/public/about-schwab/",
                 "json_req":False

             }

]


current_year=2024

def get_all_companys_data():
  all_comany_pdfs=defaultdict(lambda:[])
  data=[]

  for comany in company_list:
    obj=etree.HTML(str(BeautifulSoup('', "html.parser")))
                   
    if(comany["json_req"]):
      obj,is_success=get_json_object(comany["json_url"],HEADERS)
      logger.info(obj)
    else:
      obj,is_success=get_dom_object(comany["url"],HEADERS)
      
    if(is_success):
      data=get_company_data(obj,comany,current_year,comany["json_req"])

    all_comany_pdfs[comany["Code"]]=data

  return all_comany_pdfs



def extract_pages_with_text_pypdf(pdf_url, search_text_thresholds):
    matching_pages = []
    complete_text=''
    try:
        response = requests.get(pdf_url,headers=HEADERS,stream=True)
        print(response)
        if response.status_code != 200:
            print("Failed to fetch the PDF from the URL.")
            return matching_pages
        
        pdf_content = BytesIO(response.content)
        
        pdf_reader = PyPDF2.PdfReader(pdf_content)
            
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

                    #print(page_number,similarity,search_text,threshold)
                    
            if highest_threshold > s:
                complete_text+=page_text
                matching_pages.append(page_number )
    except:
        print()
    
            
    return matching_pages,complete_text


# Function to save PDF file and extract text
def extract_pages_with_text_llama_parse(url_path, search_text_thresholds,parse=False):
    pages_to_save=[]
    text=""

    # Requesting PDF content from the URL
    response = requests.get(url_path,headers=HEADERS,stream=True)
    pdf_data = response.content

    # Initializing PDF reader and writer objects
    pdf_writer = PyPDF2.PdfWriter()
    pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_data))

    # Iterating through each page of the PDF
    for page_number in range(len(pdf_reader.pages)):
        text = pdf_reader.pages[page_number].extract_text().lower()
        highest_threshold = 0
        s=0
        # Calculating similarity thresholds
        for search_text, threshold in search_text_thresholds:
            s+=threshold
            if search_text.lower() in text:
                highest_threshold += 100
            else:
                similarity = fuzz.partial_ratio(search_text.lower(), text)
                if similarity >= threshold:
                    highest_threshold += max(threshold, similarity)

        # Adding page to save if similarity threshold is met
        if highest_threshold>s:
            pdf_writer.add_page(pdf_reader.pages[page_number])
            pages_to_save.append(page_number )

    #print(pages_to_save)
            
    # Writing selected pages to a temporary PDF file
    output_filename = './temp.pdf'
    with open(output_filename,'wb') as out:
        pdf_writer.write(out)

    text=""

    
    # Extracting text from PDF pages if requested
    if(parse):
      documents = parser.load_data('./temp.pdf')
      #print(documents,pages_to_save)  
      for i in range(len(documents)):
          text+=documents[i].text[:]
   
    return pages_to_save,text
