import abc
import os
import logging

from mcp.client.streamable_http import streamablehttp_client
from strands import Agent
from strands.models.openai import OpenAIModel
from strands.tools.mcp import MCPClient

logger = logging.getLogger(__name__)

system_prompt = """
You are a helpful customer service bot for freshmart.

Freshmart offers loyalty tiers and Gold status is achieved after spending $1000 in a calendar year.
"""


class FreshmartAgent(abc.ABC):
    @classmethod
    def load_agent(cls):
        logger.info("Loading agent...")
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.info("No OpenAI API key found, using MockAgent")
            return MockAgent()
        else:
            logger.info("OpenAI API key found, using RealAgent")
            return RealAgent(api_key)

    @abc.abstractmethod
    def just_rag(self): ...

    @abc.abstractmethod
    def with_tools(self): ...


class RealAgent(FreshmartAgent):
    def __init__(self, api_key):
        logger.info("Initializing RealAgent...")
        self.model = OpenAIModel(
            client_args={
                "api_key": api_key,
            },
            model_id="gpt-4o",
        )
        logger.info("OpenAI model initialized")

        logger.info("Connecting to MCP server...")
        materialize = MCPClient(
            lambda: streamablehttp_client("http://mcp-materialize-server:8001/mcp")
        )
        materialize.__enter__()
        logger.info("MCP client connected")

        logger.info("Loading tools...")
        self.tools = materialize.list_tools_sync()
        logger.info(f"Loaded {len(self.tools)} tools")

    def just_rag(self):
        logger.info("Running just_rag method...")
        agent = Agent(
            model=self.model,
            system_prompt=system_prompt,
        )
        response = agent("You are helping customer 1. When will I reach gold status?")
        return response.message["content"][0]["text"]

    def with_tools(self):
        logger.info("Running with_tools method...")
        agent = Agent(
            model=self.model,
            tools=self.tools,
            system_prompt=system_prompt,
        )
        response = agent("You are helping customer 1. When will I reach gold status?")
        return response.message["content"][0]["text"]


class MockAgent(FreshmartAgent):
    def just_rag(self):
        logger.info("MockAgent just_rag called")
        return "Based on our membership program guidelines, Gold status is achieved when you spend $1,000 or more within a calendar year. For your specific progress towards Gold status, I recommend checking your account dashboard for the most up-to-date information."

    def with_tools(self):
        logger.info("MockAgent with_tools called")
        return "Based on your purchase history, you've spent $995 this year and need just $5 more to reach Gold status. With the current items in your cart (141 items totaling $151.61), you're going to reach Gold status at checkout!"
