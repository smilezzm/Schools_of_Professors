from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, Optional

import requests

from .config import DEFAULT_DEEPSEEK_ENDPOINT, DEFAULT_DEEPSEEK_MODEL


class DeepSeekClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        endpoint: str = DEFAULT_DEEPSEEK_ENDPOINT,
        model: str = DEFAULT_DEEPSEEK_MODEL,
        timeout: int = 60,
        max_retries: int = 4,
    ) -> None:
        self._load_env_file_if_needed()
        self.api_key = api_key or os.getenv("DEEPSEEK_API_KEY", "").strip()
        self.endpoint = endpoint
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()

    @staticmethod
    def _load_env_file_if_needed() -> None:
        if os.getenv("DEEPSEEK_API_KEY", "").strip():
            return
        root = Path(__file__).resolve().parents[2]
        env_path = root / ".env"
        if not env_path.exists():
            return
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def chat_json(self, prompt: str, temperature: float = 0.1) -> str:
        if not self.enabled:
            raise RuntimeError("DEEPSEEK_API_KEY is not configured")

        payload: Dict[str, object] = {
            "model": self.model,
            "temperature": temperature,
            "messages": [
                {
                    "role": "system",
                    "content": "You are a precise information extractor. Return JSON only.",
                },
                {"role": "user", "content": prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.post(
                    self.endpoint,
                    headers=headers,
                    json=payload,
                    timeout=self.timeout,
                )
                if response.status_code >= 500:
                    response.raise_for_status()

                if response.status_code in (429, 408):
                    wait_seconds = min(12, 2 * attempt)
                    time.sleep(wait_seconds)
                    continue

                response.raise_for_status()
                body = response.json()
                choices = body.get("choices") or []
                if not choices:
                    raise RuntimeError("DeepSeek response missing choices")
                message = choices[0].get("message") or {}
                content = message.get("content")
                if isinstance(content, list):
                    text_parts = [
                        part.get("text", "")
                        for part in content
                        if isinstance(part, dict) and part.get("type") == "text"
                    ]
                    return "\n".join(text_parts).strip()
                return str(content or "").strip()
            except Exception as exc:
                last_error = exc
                time.sleep(min(10, 1.5 * attempt))

        raise RuntimeError(f"DeepSeek call failed after retries: {last_error}")
