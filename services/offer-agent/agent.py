from langchain.agents import create_agent
from langchain_groq import ChatGroq
from tools import (
    get_user_profile,
    get_eligible_offers,
    check_business_constraints,
    score_relevance
)

# System prompt that guides the agent's reasoning
SYSTEM_PROMPT = (
    "You are a senior personalization straategist at a top e-commerce company. "
    "Your goal is to select the single best offer for a customer at checkout to maximize conversion without eroding margin. "
    "Use the available tools to gather data, reason step by step, and explain your decision."
)

def build_agent(db_pool, redis_client):
    """
    Build the LangChain agent with Groq LLM and personalization tools.
    """
    # Initialize the Groq LLM with Llama 3.3 70B
    llm = ChatGroq(
        model = "llama-3.3-70b-versatile",
        temperature = 0
    )
    
    # Define the tools the agent can use
    tools = [
        get_user_profile,
        get_eligible_offers,
        check_business_constraints,
        score_relevance
    ]
    
    # Create the agent with model, tools and system prompt
    agent = create_agent(
        model = llm,
        tools = tools,
        system_prompt = SYSTEM_PROMPT
    )
    
    return agent