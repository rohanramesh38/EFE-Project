# Edelman Financial Engines Wealth Management Data Extraction Project

## Overview
Edelman Financial Engines is a leading nationwide financial planning and wealth management company. To better understand client demands and behaviors on a large scale, it is crucial to gather data from competitors in the finance industry. This project aims to develop an automated process to extract wealth management data from major investment banks in the U.S. using a large language model.

## Objective
The primary objective of this project is to automate the extraction of wealth management data from quarterly reports of top competitors. Currently, Edelman manually collects this data from publicly available reports, which is time-consuming and inefficient. By leveraging Python for web scraping and utilizing language models such as Claude 2 and Llama 2, we aim to improve the efficiency of this process.

## Approach
1. **Web Scraping**: Python scripts will be used to scrape quarterly reports directly from each company's website. This eliminates the need for manual data extraction.
2. **Model Comparison**: We will employ Claude 2 and Llama 2, two large language models, to extract data from the scraped reports. By comparing the performance of these models, we aim to determine which one better suits our needs,Extracted data will be analyzed to gather important financial metrics .
   
## Benefits
- **Efficiency**: Automating the data extraction process will save time and resources previously spent on manual collection.
- **Accuracy**: By using language models for extraction, we aim to improve the accuracy of gathered data.
- **Insights**: Access to timely and accurate financial data will enable Edelman to make informed decisions and adapt to changing market trends effectively.

## Future Work
- **Model Optimization**: Continuously improving the extraction models to enhance accuracy and efficiency.
- **Integration**: Integrating the automated extraction process into Edelman's existing systems for seamless data management.
- **Expansion**: Exploring opportunities to expand data extraction to include additional competitors or sources.

## Contributors
- Christian Miller
- Rohan Ponramesh
- Nat Promma
- Shuangyu Zhao

## Requirements

Use `pip install -f requirements.txt` to install the necessary dependencies.

To use Llama parse, create an API Key at [https://cloud.llamaindex.ai/parse](https://cloud.llamaindex.ai/parse).

To set up AWS Boto3 in a local environment, refer to [Medium blog aws botto3 setup](https://medium.com/genai-io/aws-bedrock-quick-setup-with-boto3-94ba0d0088ca).


