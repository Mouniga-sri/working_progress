import pandas as pd
import numpy as np  # Import numpy to handle NaN values
from kor.extraction import create_extraction_chain
from kor.nodes import Object, Text
from langchain_openai import AzureChatOpenAI
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Initialize Azure OpenAI environment variables
os.environ["AZURE_OPENAI_ENDPOINT"] = str(os.getenv("AZURE_OPENAI_ENDPOINT"))
os.environ["AZURE_OPENAI_API_KEY"] = str(os.getenv("AZURE_OPENAI_KEY"))

# Initialize Azure OpenAI LLM
llm = AzureChatOpenAI(
    openai_api_version="2024-05-01-preview",
    azure_deployment="gpt-4o",
)

# Define the schema
schema = Object(
    id="template_details",
    description="Extract and modify observations, variables, and questions based on new observations. If observation changed, find any variables that are added and update.",
    attributes=[
        Text(
            id="observation",
            description="Updated observation based on new input.",
        ),
        Text(
            id="impression",
            description="Updated impression based on new input.",
        ),
        Text(
            id="variables",
            description="Updated variables extracted from the new observation.",
        ),
        Text(
            id="question",
            description="Updated question JSON based on the new observation.",
        ),
    ],
    examples=[
        (
            "Existing report: ascites noted. New observation: mild ascites noted.",
            {
                "observation": "mild ascites noted.",
                "impression": "",
                "variables": "fluid_level",
                "question": """
                [{
                    "type": "heading",
                    "key": "",
                    "value": "Ascites"
                },
                {
                    "type": "single choice",
                    "key": "fluid_level",
                    "place_holder": "false",
                    "value": "Severity level of ascites noted",
                    "followup": "false",
                    "to_followup": "NA"
                }]
                """
            },
        ),
    ],
    many=False,
)

# Create extraction chain
chain = create_extraction_chain(llm, schema)

print(chain.prompt.format_prompt(text="[user_input]".to_string()))

# Function to analyze and update the report
def analyze_and_update_report(pathology, new_observation, file_path='reports.csv'):
    """
    Analyze the new observation and update the corresponding row for the pathology.

    Args:
        pathology (str): The pathology to fetch from the DataFrame.
        new_observation (str): The new observation to analyze.
        file_path (str): Path to the CSV file containing pathology reports. Defaults to 'reports.csv'.

    Returns:
        dict: A dictionary with updated observation, impression, variables, and question.
    """
    # Load the CSV file
    df = pd.read_csv(file_path)

    # Fetch the row for the given pathology (case insensitive)
    row = df[df['PATHOLOGIES'].str.lower() == pathology.lower()]

    if row.empty:
        raise ValueError(f"Pathology '{pathology}' not found in the DataFrame.")

    # Extract existing data from the first matching row
    existing_observation = row['OBSERVATION'].iloc[0]
    existing_impression = row['IMPRESSION'].iloc[0]
    existing_variables = row['VARIABLES'].iloc[0]
    existing_question = row['QUESTION'].iloc[0]

    # Input for the chain: Provide explicit instructions to the LLM
    input_data = (
        f"Existing report: {existing_observation}. "
        f"Existing impression: {existing_impression}. "
        f"Existing variables: {existing_variables}. "
        f"Existing question: {existing_question}. "
        f"New observation: {new_observation}. "
        "act as a radiology report generator."
        "Please analyze this new observation and extract any possible variables it suggests."
        "decide is there any variables present in a new observation"
        "add a variable in observation template by adding '_' before the variable value"
        "Also, generate a new question JSON based on this new observation. "
        "If new variables are identified, include them in the variables field and create a corresponding question."
    )

    # Invoke the chain to get the updated details
    result = chain.invoke(input_data)

    # Check if the new observation is different from the existing one
    final_observation = new_observation if new_observation != existing_observation else existing_observation

    # Handle missing variables or questions (i.e., return np.nan for undefined)
    updated_variables = result.get("variables", np.nan)
    updated_question = result.get("question", np.nan)

    # Format the result
    return {
        "final_observation": final_observation,  # Use new observation if it differs
        "final_impression": result.get("impression", existing_impression),
        "final_variables": updated_variables if pd.notna(updated_variables) else np.nan,
        "final_question": updated_question if pd.notna(updated_question) else np.nan,
    }


# Example usage
updated_report = analyze_and_update_report(
    pathology="bulky cervix", 
    new_observation="Cervix appears bulky in size and heterogeneous in echotexture and measures 23x21mm"
)

print(updated_report)
