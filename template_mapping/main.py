from langchain_openai import AzureChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import AzureOpenAIEmbeddings
from dotenv import load_dotenv
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor  

load_dotenv()

print(os.getenv("AZURE_OPENAI_ENDPOINT"))

print(os.getenv("AZURE_OPENAI_KEY"))


os.environ["AZURE_OPENAI_ENDPOINT"] = str(os.getenv("AZURE_OPENAI_ENDPOINT"))
os.environ["AZURE_OPENAI_API_KEY"] = str(os.getenv("AZURE_OPENAI_KEY"))

llm = AzureChatOpenAI(
    openai_api_version= "2024-05-01-preview",
    azure_deployment= 'gpt-4o',
)

# print(llm.invoke("hello"))

# Load CSV data
findings_df = pd.read_csv('test - Sheet2.csv')
pathologies_df = pd.read_csv('test - Sheet1.csv')

result_df = findings_df.copy()

# Function to get the AI response for a given pathology and finding
def get_ai_response(pathology, finding):

    placeholder = findings_df['placeholder']
    observation = pathologies_df['OBSERVATION']

    prompt = f"""
You are a radiology report generator. Given the following normal findings: {findings_df['findings'].tolist()},
analyze the findings based on the pathology '{pathology}' and evaluate their relevance considering the corresponding {placeholder} and {observation} template.

Rules:
Identify the specific placeholder related to the pathology from the pathology sheet. Only mark the placeholder directly related to the pathology as 'no'.
Do not mark other placeholders as 'no'.
1. Strictly follow, If the finding is related to (female) uterus/ovaries, then the prostate placeholder finding should be marked as 'no'.
2. Strictly follow, If the finding is related to the prostate, then the uterus/ovaries placeholder finding should be marked as 'no'.
3. If the finding is directly related to the pathology, mark it as 'no'.
4. If the finding is irrelevant to the pathology or unrelated to its placeholder, mark it as 'yes'.
5. Only change 'no' for the placeholder directly related to the pathology. If other placeholders are not directly impacted by the pathology, leave them as 'yes' unless they are significantly important for the report.


Finding: {finding}

Task:
- Identify the relevant placeholder for the pathology and determine if the finding should remain in the report based on its relationship to the pathology and placeholder.
- Respond strictly with 'yes' or 'no' based on the rules above. Provide no additional text.
"""


    response = llm.invoke(prompt)

    response_text = response.content
    return response_text.strip().lower()

# Function to process a single pathology
def process_pathology(pathology):
    result_df[pathology] = result_df['findings'].apply(lambda x: get_ai_response(pathology, x))




    return result_df[pathology]

# Function to process all pathologies
def all_pathology():
    with ThreadPoolExecutor(max_workers=16) as exe:
        futures = [exe.submit(process_pathology, pathology) for pathology in pathologies_df['PATHOLOGIES']]

        for future in futures:
            future.result()

    result_df.to_csv('mapping_result2.csv', index=False)


all_pathology()
