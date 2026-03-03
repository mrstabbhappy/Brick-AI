import os
import logging
from typing import Any, Dict, List
from urllib.parse import urlparse

try:
    from openai import AzureOpenAI, OpenAI
except Exception:
    AzureOpenAI = None
    OpenAI = None

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self):
        self.client = None
        self.deployment = None
        self.is_azure = False
        self._init_client()

    def _init_client(self) -> None:
        azure_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT") or os.getenv("AOAI_ENDPOINT")
        azure_key = os.getenv("AZURE_OPENAI_API_KEY") or os.getenv("AOAI_API_KEY")
        azure_deployment = (
            os.getenv("AZURE_OPENAI_DEPLOYMENT")
            or os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
            or os.getenv("AOAI_DEPLOYMENT")
        )
        azure_version = os.getenv("AZURE_OPENAI_API_VERSION") or os.getenv("AOAI_API_VERSION", "2024-06-01")

        if azure_endpoint:
                                                                             
                                                        
            try:
                parsed = urlparse(azure_endpoint)
                if parsed.scheme and parsed.netloc:
                    azure_endpoint = f"{parsed.scheme}://{parsed.netloc}/"
            except Exception:
                pass

        if all([azure_endpoint, azure_key, azure_deployment]) and AzureOpenAI:
            try:
                self.client = AzureOpenAI(
                    api_key=azure_key,
                    api_version=azure_version,
                    azure_endpoint=azure_endpoint,
                )
                self.deployment = azure_deployment
                self.is_azure = True
                return
            except Exception as e:
                logger.warning(f"Azure OpenAI init failed: {e}")

        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and OpenAI:
            try:
                self.client = OpenAI(api_key=openai_key)
                self.deployment = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
                self.is_azure = False
                return
            except Exception as e:
                logger.warning(f"OpenAI init failed: {e}")

    def chat(self, messages: List[Dict[str, str]], max_tokens: int = 600, temperature: float = 0.2, **kwargs) -> str:
        if not self.client:
            return "AI is not configured. You can still browse the catalog and use image search."

        try:
            resp = self.client.chat.completions.create(
                model=self.deployment,
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
                **kwargs,
            )
            content = resp.choices[0].message.content
            return (content or "").strip() or "No response."
        except Exception as e:
                                                                       
                                                                 
            name = type(e).__name__
            logger.warning(f"LLM chat failed ({name}): {e}")
            if name == "AuthenticationError":
                return (
                    "The AI assistant is not authenticated. "
                    "Check your OPENAI_API_KEY (or Azure OpenAI env vars) in .env and restart the app."
                )

            if name == "PermissionDeniedError":
                msg = str(e)
                if "Virtual Network/Firewall" in msg or "firewall" in msg.lower() or "vnet" in msg.lower():
                    return (
                        "The AI assistant is blocked by Azure OpenAI network security (VNet/Firewall rules). "
                        "Enable public network access or add your client IP in the Azure OpenAI resource Networking settings "
                        "(or connect through the approved VNet/VPN), then retry."
                    )
                return (
                    "The AI assistant does not have permission to call Azure OpenAI. "
                    "Verify the resource Networking settings and that the API key belongs to this Azure OpenAI resource."
                )
            return "The AI assistant is temporarily unavailable. Try again in a moment."

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.client is not None,
            "provider": "Azure OpenAI" if self.is_azure else ("OpenAI" if self.client else "None"),
            "model": self.deployment if self.client else None,
        }


_llm: LLMService | None = None


def get_llm_service() -> LLMService:
    global _llm
    if _llm is None:
        _llm = LLMService()
    return _llm
