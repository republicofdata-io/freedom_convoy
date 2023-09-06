from dagster import resource, StringSource
from langchain.chat_models import ChatOpenAI

class OpenAIResource:
    def __init__(self, openai_api_key):
        self._openai_api_key = openai_api_key
        self._chat = ChatOpenAI(openai_api_key, 
                                temperature=1)


@resource(
    config_schema={
        "openai_api_key": StringSource
    },
    description="A OpenAI resource."
)
def initiate_openai_resource(context):
    return OpenAIResource(
        openai_api_key = context.resource_config["openai_api_key"]
    )
