
import requests
from bs4 import BeautifulSoup
from lxml import etree
import wget
import numpy as np
from fuzzywuzzy import fuzz


def get_dom_object(url,HEADERS):

  response = requests.get(url,HEADERS)
  #print(url,response.status_code )
  soup = BeautifulSoup(response.content, "html.parser")
  dom = etree.HTML(str(soup))

  return dom,(response.status_code == 200)

def download_pdf(url, save_path):
    response = requests.get(url)
    with open(save_path, 'wb') as pdf_file:
        pdf_file.write(response.content)

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
      #print(report_url)
      return report_url,(part!=None)

  if(company_code=="BOFA"):
    element_list=dom.xpath(xpath)

    if(element_list):
      element=element_list[0]
      report_url=element.attrib["href"]
      #print(etree.tostring(element))
      text='random'
      try:
        text=element.attrib["aria-label"]
      except:
        text='random'

      is_sumplimental =True if fuzz.partial_ratio(text.lower(),"supplemental information")>=90 else False

      return report_url,is_sumplimental


def get_company_data(dom,comany,current_year):

  data={}
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

  return data

company_list=[

             {   "Name":"Morgan Stanley",
                 "Code":"MGS",
                 "report_avalaible_from":2023,
                 "report_avalaible_till":2002,
                 "url":"https://www.morganstanley.com/about-us-ir/earnings-releases",
                 "base_url":"https://www.morganstanley.com"
             },
              {  "Name":"Bank of America",
                 "Code":"BOFA",
                 "report_avalaible_from":2023,
                 "report_avalaible_till":2018,
                 "url":"https://investor.bankofamerica.com/quarterly-earnings",
                 "base_url":"https://investor.bankofamerica.com/quarterly-earnings"
             }

]

HEADERS = ({'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \ (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36','Accept-Language': 'en-US, en;q=0.5'})

current_year=2024

def get_all_companys_data():
  all_comany_pdfs={}
  data=[]
  for comany in company_list:
    dom,is_success=get_dom_object(comany["url"],HEADERS)
    if(is_success):
      data=get_company_data(dom,comany,current_year)

    all_comany_pdfs[comany["Code"]]=data

  return all_comany_pdfs



