import pandas as pd
import numpy as np  # For handling NaN values
from kor.extraction import create_extraction_chain
from kor.nodes import Object, Text
from langchain_community.chat_models import AzureChatOpenAI
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

os.environ["AZURE_OPENAI_ENDPOINT"] = str(os.getenv("AZURE_OPENAI_ENDPOINT"))
os.environ["AZURE_OPENAI_API_KEY"] = str(os.getenv("AZURE_OPENAI_KEY"))

# Initialize Azure OpenAI with LangChain
llm = AzureChatOpenAI(
    model="gpt-4",
    openai_api_key=os.getenv("AZURE_OPENAI_KEY"),
    api_base=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2024-05-01-preview",
    engine="gpt-4o",  # Use your Azure deployment name
)

def process_pathology(pathology, new_observation, csv_file_path="reports.csv"):
    """
    Process a given pathology and new observation, and return updated JSON data.

    Parameters:
        pathology (str): Pathology name to filter the row.
        new_observation (str): The updated observation text.
        csv_file_path (str): Path to the CSV file.

    Returns:
        dict: JSON output with updated observation, variables, and questions.
    """
    # Load the CSV file
    df = pd.read_csv(csv_file_path)
    
    # Filter the row for the given pathology
    pathology_row = df[df['PATHOLOGIES'].str.lower() == pathology.lower()]
    
    if pathology_row.empty:
        return {"error": "Pathology not found in the dataset."}
    
    # Extract old observation, variables, and questions
    old_observation = pathology_row['OBSERVATION'].values[0] or ""
    old_variables = pathology_row['VARIABLES'].values[0] or ""
    old_question = pathology_row['QUESTION'].values[0] or ""
    
    # Compare old and new observation
    if old_observation.strip() == new_observation.strip():
        return {"error": "No changes detected in the observation."}
    
    # Generate prompt for LLM to extract changes and suggest variables/questions
    prompt = f"""
    The old observation is: '{old_observation}'.
    The new observation is: '{new_observation}'.
    Identify the changes and generate:
    - New observation template with variables indicated by underscores (e.g., '_variable_name').
    - List of new variables, their types (input box/single choice/multiple choice), and values (e.g., right/left/bilateral).
    - A structured question format based on the observation template.

    Provide the result in JSON format with keys:
    - "new_observation"
    - "new_variables" (comma-separated names, types, and values).
    - "new_question" (comma-separated details: types, keys, placeholders, texts, follow-ups, and follow-up details).
    """
    
    # Define output schema without using Array
    output_schema = Object(
        id="output",
        description="Structured output for the updated observation, variables, and questions.",
        attributes=[
            Text(id="new_observation", description="Updated observation template with variables."),
            Object(
                id="new_variables",
                description="Details of new variables.",
                attributes=[
                    Text(id="names", description="Variable names, comma-separated."),
                    Text(id="types", description="Variable types (e.g., input box, single choice), comma-separated."),
                    Text(id="values", description="Possible values for variables, comma-separated."),
                ],
            ),
            Object(
                id="new_question",
                description="Details of the updated questions.",
                attributes=[
                    Text(id="types", description="Question types, comma-separated."),
                    Text(id="keys", description="Keys for the questions, comma-separated."),
                    Text(id="place_holders", description="Placeholders for the questions, comma-separated."),
                    Text(id="values", description="Question texts, comma-separated."),
                    Text(id="followups", description="Whether follow-ups are required, comma-separated."),
                    Text(id="to_followups", description="Details of follow-ups, comma-separated."),
                ],
            ),
        ],
    )
    
    # Create extraction chain
    extraction_chain = create_extraction_chain(output_schema, llm)
    
    # Run the extraction
    result = extraction_chain.run(prompt)
    
    # Parse variables
    variables = result['new_variables']
    variable_names = variables['names'].split(',')
    variable_types = variables['types'].split(',')
    variable_values = variables['values'].split(',')
    
    # Parse questions
    questions = result['new_question']
    question_types = questions['types'].split(',')
    question_keys = questions['keys'].split(',')
    question_placeholders = questions['place_holders'].split(',')
    question_texts = questions['values'].split(',')
    question_followups = questions['followups'].split(',')
    question_to_followups = questions['to_followups'].split(',')
    
    # Construct updated question list
    updated_questions = [
        {
            "type": q_type.strip(),
            "key": q_key.strip(),
            "place_holder": q_placeholder.strip(),
            "value": q_text.strip(),
            "followup": q_followup.strip(),
            "to_followup": q_to_followup.strip(),
        }
        for q_type, q_key, q_placeholder, q_text, q_followup, q_to_followup in zip(
            question_types, question_keys, question_placeholders, question_texts, question_followups, question_to_followups
        )
    ]
    
    # Construct final output
    final_output = {
        "new_observation": result['new_observation'],
        "new_variables": [
            {
                "name": name.strip(),
                "type": v_type.strip(),
                "values": v_values.strip(),
            }
            for name, v_type, v_values in zip(variable_names, variable_types, variable_values)
        ],
        "new_question": updated_questions,
    }
    
    # Update the CSV with new values
    df.loc[df['PATHOLOGIES'].str.lower() == pathology.lower(), 'OBSERVATION'] = result['new_observation']
    df.loc[df['PATHOLOGIES'].str.lower() == pathology.lower(), 'VARIABLES'] = "; ".join(variable_names)
    df.loc[df['PATHOLOGIES'].str.lower() == pathology.lower(), 'QUESTION'] = str(updated_questions)
    
    # Save updated CSV
    df.to_csv(csv_file_path, index=False)
    
    return final_output


# Example usage
pathology_input = "bulky cervix"
new_observation_input = "bulky cervix noted and measures 23 mm."

output = process_pathology(pathology_input, new_observation_input)
print(output)
