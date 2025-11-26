from openai import AsyncOpenAI
from app.core.config import settings


class OpenAIClientSingleton:
    """
    Singleton pattern for OpenAI client to avoid multiple initializations.
    """
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OpenAIClientSingleton, cls).__new__(cls)
            cls._client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
        return cls._instance
    
    @property
    def client(self) -> AsyncOpenAI:
        """Returns the OpenAI async client instance."""
        return self._client


# Global client instance
openai_client = OpenAIClientSingleton().client