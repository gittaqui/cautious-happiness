import logging
import azure.functions as func
import openai
import os
import json
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
import numpy
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

# Set your GPT Engine, Kusto Cluster, and Database here
GPT_ENGINE = "gpt-4-32k"
KUSTO_CLUSTER = os.environ.get('KUSTO_CLUSTER')  # Set as environment variable in Azure
KUSTO_DATABASE = os.environ.get('KUSTO_DATABASE')
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')

# Configure OpenAI settings
openai.api_type = "azure"
openai.api_key = OPENAI_API_KEY  # Your OpenAI API key
openai.api_base = "https://mdee-seot3-aoai.openai.azure.com/"  # Your OpenAI resource endpoint
openai.api_version = "2023-06-01-preview"

def run_openai(prompt, engine=GPT_ENGINE):
    """Generate Kusto Query Language (KQL) queries using OpenAI's GPT-4.0 model."""
    system_prompt = f"""You are a helpful assistant that generates valid Kusto queries, make use of three table Event, Heartbeat and Perf, all at once or silo as per need. Generate one single query, no additional explanation as the query will be directly executed in the Kusto explorer through, so additional comment or invalid character might result in error message
                      // List all known computers that didn't send a heartbeat in the last 24 hours from heartbeat table
Heartbeat
| summarize LastHeartbeat=max(TimeGenerated) by Computer
| where LastHeartbeat < ago(24h)

// Top 10 computers with the highest disk space from Perf table
// Show the top 10 computers with the highest available disk space from Perf table
Perf
| where CounterName == "Free Megabytes" and InstanceName == "_Total"
| summarize arg_max(TimeGenerated, *) by Computer
| top 10 by CounterValue

// Top 10 Computers with Max CPU usage from Perf table
// etc..."""

    full_prompt = f"{system_prompt}\n{prompt}"
    response = openai.Completion.create(
        engine=engine,
        prompt=full_prompt,
        temperature=0.7,
        max_tokens=150
    )
    return response.choices[0].text

def execute_kusto_query(kusto_cluster, kusto_database, query):
    """Executes a KQL query on the specified Kusto Cluster and Database."""
    print(query)
    logging.info("execute Kusto")
    kusto_client = KustoClient(KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(KUSTO_CLUSTER))
    logging.info('About to Execute Query.')
    response = kusto_client.execute(KUSTO_DATABASE, query)
    return response.primary_results[0] if response else None

def get_query(nlquery):
    """Prepares the query for OpenAI based on natural language input."""
    logging.info("get query")
    return f"""
    generate only one query (no additional explanation as the query will be directly executed in the Kusto explorer through, so additional comment or invalid character might result in error message)in Kusto format for the following question: {nlquery}.
    There are three table with following columns
    First table name is Event with columns TenantId,SourceSystem,TimeGenerated [UTC],Source,EventLog,Computer,EventLevel,EventLevelName,ParameterXml,EventData,EventID,RenderedDescription,AzureDeploymentID,Role,EventCategory,UserName,Message,MG,ManagementGroupName,Type,_ResourceId. Second table name is Heartbeat and Heartbeat has following columns TenantId,SourceSystem,TimeGenerated [UTC],MG,ManagementGroupName,SourceComputerId,ComputerIP,Computer,Category,OSType,OSName,OSMajorVersion,OSMinorVersion,Version,SCAgentChannel,IsGatewayInstalled,RemoteIPLongitude,RemoteIPLatitude,RemoteIPCountry,SubscriptionId,ResourceGroup,ResourceProvider,Resource,ResourceId,ResourceType,ComputerEnvironment,Solutions,VMUUID,ComputerPrivateIPs,Type,_ResourceId.
    Third table name is Perf with column names TenantId,Computer,ObjectName,CounterName,InstanceName,Min,Max,SampleCount,CounterValue,TimeGenerated [UTC],BucketStartTime [UTC],BucketEndTime [UTC],SourceSystem,CounterPath,StandardDeviation,MG,Type,_ResourceId.
    Validate and create valid KQL queries. Some of the sample queries that could be return without explanation are  are below, 
    """

st.title('KQL Query Writing and Data Analysis Assistant')

# User inputs for Kusto cluster and database
kusto_cluster = st.text_input("Enter Kusto Cluster URL:")
kusto_database = st.text_input("Enter Kusto Database Name:")

question = st.text_area("Enter your business question or use a question from the FAQ:")
show_code = st.checkbox("Show KQL Query")
show_prompt = st.checkbox("Show GPT-4 Prompt")
advanced_analysis = st.checkbox("Advanced Data Analysis (Forecasting)")

if st.button("Submit") and kusto_cluster and kusto_database:
    openai_query = get_query(question)
    if show_prompt:
        st.write("GPT-4 Prompt:")
        st.write(question)

    kql_query = run_openai(openai_query)
    if show_code:
        st.write("Generated KQL Query:")
        st.code(kql_query)

    kusto_response = execute_kusto_query(kusto_cluster, kusto_database, kql_query)
    if kusto_response is not None:
        st.write("Query Results:")
        st.dataframe(kusto_response)

        if advanced_analysis:
            # Advanced analysis logic here, e.g., forecasting
            st.write("Advanced Analysis:")
            plt.figure()
            plt.plot(kusto_response['Date'], kusto_response['Value'])
            plt.xlabel('Date')
            plt.ylabel('Value')
            plt.title('Data Over Time')
            st.pyplot(plt)
    else:
        st.write("An error occurred while executing the Kusto query.")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        nlquery = req.params.get("nlquery")
        if not nlquery:
            return func.HttpResponse(
                "Please pass a nlquery parameter in the query string or in the request body.",
                status_code=400
            )

        openai_query = get_query(nlquery)
        kql_query = run_openai(openai_query)
        kusto_response = execute_kusto_query(kql_query)

        if kusto_response:
            result_df = dataframe_from_result_table(kusto_response)
            json_result = result_df.to_json(orient="records")

            return func.HttpResponse(
                json.dumps({"kql_query": kql_query, "kusto_result": json_result}),
                status_code=200,
                mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                "An error occurred while executing the Kusto query.",
                status_code=500
            )

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        return func.HttpResponse(
            f"An error occurred: {str(e)}",
            status_code=500
        )
