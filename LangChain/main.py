
import os
import logging
from dotenv import load_dotenv

from langchain.chat_models import ChatDatabricks
from langchain.prompts import ChatPromptTemplate


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load environment variables from .env file
load_dotenv()
dbtoken = os.getenv('DATABRICKS_TOKEN')
db_workspace = os.environ.get('DATABRICKS_HOST')
model_name = "databricks-dbrx-instruct"
endpoint_url = f"https://{db_workspace}/serving-endpoints/{model_name}/invocations"


llm_model = ChatDatabricks(endpoint=model_name, api_token=dbtoken, host=db_workspace ,max_tokens = 200, temperature=0.1, )
prompt = ChatPromptTemplate.from_template("Follow the instruction: {question}")

chain = llm_model | prompt

chain.invoke("Please explain y=mx+b")