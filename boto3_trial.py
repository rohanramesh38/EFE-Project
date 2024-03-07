import boto3

import json
import os

from fuzzywuzzy import fuzz

"""
Purpose

 AWS SDK for Python (Boto3) with Amazon Bedrock to manage
Bedrock models.
"""
import PyPDF2

import logging
import boto3
from botocore.exceptions import ClientError
import fitz

def convert_pdf_to_text(pdf_path,selected_pages):
    pdf_document = fitz.open(pdf_path)
    
    text = ""
    
    for page_number in range(pdf_document.page_count):
        if(page_number in selected_pages):
            page = pdf_document[page_number]
            text += page.get_text()
    
    pdf_document.close()
    
    return text

logger = logging.getLogger(__name__)

aws_access_key_id = os.environ.get('aws_access_key_id')
aws_secret_access_key = os.environ.get('aws_secret_access_key')
class BedrockWrapper:
    """Encapsulates Amazon Bedrock foundation model actions."""

    def __init__(self, bedrock_client):
        self.bedrock_client = bedrock_client


    def list_foundation_models(self):
        """
        List the available Amazon Bedrock foundation models.

        :return: The list of available bedrock foundation models.
        """

        try:
            response = self.bedrock_client.list_foundation_models()
            models = response["modelSummaries"]
            logger.info("Got %s foundation models.", len(models))
        except ClientError:
            logger.error("Couldn't list foundation models.")
            raise
        else:
            return models
def extract_pages_with_text(pdf_path, search_texts, threshold=80,skip=0):
    matching_pages = []
    page_text=''
    pdf_document = fitz.open(pdf_path)
    for page_number in range(pdf_document.page_count):
        page = pdf_document[page_number]
        page_text = str(page.get_text()).lower()
        #print(page_text)
        
        similarity = 0

        for search_text in search_texts:
            if(search_text.lower() in page_text):
                similarity+=100
            else:
                similarity+=fuzz.partial_ratio(search_text.lower(), page_text)
        value=len(search_texts)-skip

        #print(similarity,threshold,similarity >=threshold*(value))
        if similarity >= threshold*(len(search_texts)-skip):
            matching_pages.append(page_number + 1)
        #print(matching_pages)

    return matching_pages

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
    # text
    print(response_body.get('completion'))


    
    
if __name__ == "__main__":
    bedrock_client= get_runtime()
    text_from_pages=get_sample_text()

    prompt='From the given information, create a markdown table with containing wealth management information about Wealth AUS, Wealth net flows, Wealth Management Fees, Net market appreciation / (depreciation)and Total Revenues'

    data=generate_data(text_from_pages,prompt)
    
    body=generate_body(data)

    #invoke_claude_model(bedrock_client,body)

    

    schwab_url='./../Data/schwab.pdf'
    gs_gs2Q23_path='./../Data/gs2Q23.pdf'

    gs_search_words=['Asset','Earnings Results','AUS']
    schwab_search_words = ['Total advice solutions','Total net new assets' ]



    #--------------------------------------------



    matching_pages=extract_pages_with_text(gs_gs2Q23_path,gs_search_words,20,1)

    print(matching_pages)

    text=convert_pdf_to_text(gs_gs2Q23_path,matching_pages)

    data=generate_data(text,prompt)
    
    body=generate_body(data)
    #print(text)

    invoke_claude_model(bedrock_client,body)
    

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


  
