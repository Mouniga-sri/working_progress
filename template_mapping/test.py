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
# Function to get the AI response for a given pathology and finding
def get_ai_response(pathology, finding, placeholder, observation):
    prompt = f"""
You are a radiology report generator. Given the following normal findings: {finding},
analyze the findings based on the pathology '{pathology}' and evaluate their relevance considering the corresponding {placeholder} and {observation} template.

Rules:
 Identify the specific placeholder related to the pathology from the pathology sheet. Only mark the placeholder directly related to the pathology as 'no'.
 Do not mark other placeholders as 'no'.
 If the finding is directly related to the pathology, mark it as 'no'.
 If the finding is irrelevant to the pathology or unrelated to its placeholder, mark it as 'yes'.
 Only change 'no' for the placeholder directly related to the pathology. If other placeholders are not directly impacted by the pathology, leave them as 'yes' unless they are significantly important for the report.

Finding: {finding}

Task:
- Identify the relevant placeholder for the pathology and determine if the finding should remain in the report based on its relationship to the pathology and placeholder.
- Respond strictly with 'yes' or 'no' based on the rules above. Provide no additional text.
"""
    response = llm.invoke(prompt)
    return response.content.strip().lower()

# Function to process a single pathology
def process_pathology(pathology, placeholder, observation):

    temp_df = findings_df.copy()

    # Identify rows with the same placeholder
    related_rows = temp_df['placeholder'] == placeholder
    
    if placeholder.lower() in ['uterus', 'ovaries']:

        temp_df.loc[temp_df['placeholder'].str.contains('prostate', case=False, na=False), pathology] = 'no'

    elif placeholder.lower() == 'prostate':

        temp_df.loc[temp_df['placeholder'].str.contains('uterus|ovaries', case=False, na=False), pathology] = 'no'

    temp_df.loc[related_rows, pathology] = temp_df.loc[related_rows, 'findings'].apply(
        lambda finding: get_ai_response(pathology, finding, placeholder, observation)
    )

    temp_df.loc[~related_rows & temp_df[pathology].isna(), pathology] = 'yes'
    
    return temp_df[[pathology]]

# Function to process all pathologies
def all_pathology():
    result_df = findings_df.copy()
    
    with ThreadPoolExecutor(max_workers=16) as exe:
        futures = [
            exe.submit(
                process_pathology, 
                row['PATHOLOGIES'], 
                row['PLACEHOLDER'], 
                row['OBSERVATION']
            )
            for _, row in pathologies_df.iterrows()
        ]

        for future, (_, row) in zip(futures, pathologies_df.iterrows()):
            result_df[row['PATHOLOGIES']] = future.result()[row['PATHOLOGIES']]
    
    result_df.to_csv('mapping_result1.csv', index=False)

# Run the processing
all_pathology()
