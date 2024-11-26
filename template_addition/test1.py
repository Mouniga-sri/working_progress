from langchain_openai import AzureChatOpenAI
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import AzureOpenAIEmbeddings
from dotenv import load_dotenv
import os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor 

# Function to get the AI response for a single finding, organ, and pathology
def get_ai_response_for_finding(findings, organ):
    
    curr_dir = os.getcwd()
    load_dotenv(f"{curr_dir}/pwd.env")

    os.environ["AZURE_OPENAI_ENDPOINT"] = str(os.getenv("AZURE_OPENAI_ENDPOINT"))
    os.environ["AZURE_OPENAI_API_KEY"] = str(os.getenv("AZURE_OPENAI_KEY"))

    # Initialize the AzureChatOpenAI model
    llm = AzureChatOpenAI(
        openai_api_version="2024-05-01-preview",
        azure_deployment='gpt-4o',
    )
# C:\Users\MounigasriM\Documents\my_project\pathologies.csv
    csv_path = os.path.join(curr_dir,"pathologies.csv")

    df = pd.read_csv(csv_path)

    filtered_pathology =  df[df['PLACEHOLDER'].str.lower() == f'{organ}']['PATHOLOGIES'].to_list()

    prompt = f"""
    You are given a list of findings and an organ (or placeholder) name. 
    Your task is to determine if each finding in the list is directly related to a pathology given associated with the organ.

    For each pathology(give you pathology list), you will check if the finding is relevant to that pathology. 
    If the finding is relevant (i.e., directly associated with the pathology), mark it as 'no'. 
    If the finding is irrelevant or unrelated to the pathology or its placeholder, mark it as 'yes'.

    Inputs:
    1. Findings: {findings}
    2. Organ: {organ}
    3. Pathologies: {filtered_pathology}


    Outputs:
    - A list of dictionaries, where each dictionary corresponds to a pathology and contains:
      - 'pathology': The pathology name
      - 'finding': The finding from the input list

    Example input:
    findings_list = ['Finding 1', 'Finding 2', 'Finding 3']
    organ = "Lung"

    Output:
  results = {{
    "Normal in size (~ liver_size cm) and echotexture.": 
    [
        {{"pathology1": "yes", "pathology2": "yes", "pathology3": "no"}}
    ],
    "No intra hepatic duct dilatation. No obvious focal lesions.": 
    [
        {{"pathology1": "no", "pathology2": "yes", "pathology3": "no"}}
    ]
}}
    Respond strictly with the output only a dictionary with list of dicitonary(as mentioned in example output) 
    Dont give any addtional text.
    Remember: You must exclude pathologies that are unrelated to the given organ.
    Ensure all the pathologies were processed if not all pathologies are processed please process all pathologies.
    """
    response = llm.invoke(prompt)
    # print(response)
    return response.content.strip().lower()

#  example usage:  
findings = [
    "Normal in size (~ liver_size cm) and echotexture.",
    "No intra hepatic duct dilatation. No obvious focal lesions.",
    "Normal in calibre and course.",
    "bulky uterus noted",
    "Mulitple calcified fibroid noted.",
    "hepatomegaly noted"
]
organ = "portal vein"


print(get_ai_response_for_finding(findings, organ))
