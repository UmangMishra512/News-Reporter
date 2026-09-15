"""
agents/summarizer.py — Summarization Agent (LLM-powered).

Professional newsroom-style summaries. No opinions, no speculation,
no sensationalism. Supports Gemini, OpenAI, Groq, and Ollama.
"""
from __future__ import annotations

import asyncio
import json
from typing import List, Optional

from config.settings import get_settings
from core.database import get_db
from core.events import Event, EventBus, Topics, get_bus
from core.logger import AgentLogger
from core.models import AgentHeartbeat, AgentStatus, Article, Category

log = AgentLogger("summarizer")

SYSTEM_PROMPT = """You are a senior news editor for a professional India-focused newsroom.

Your task: Given a news article title and raw content, produce:
1. A polished HEADLINE (10-15 words, active voice, factual, no clickbait)
2. A SUMMARY of exactly 2-3 sentences, maximum 80 words total.

Rules:
- Use only facts present in the title and content. Do not invent anything.
- No opinions, no speculation, no emotional language.
- No first-person language.
- Do not start with "The article says" or similar meta-phrases.
- If the category is STOCK_MARKET or FINANCE, include relevant numbers/percentages.
- If the article is government/PIB, use formal language.
- If insufficient content, write what you can from the title alone.

Return ONLY valid JSON:
{
  "headline": "...",
  "summary": "...",
  "category": "one of: politics|finance|economy|business|stock_market|technology|ai|startups|sports|national_security|government|crime|accidents|natural_disasters|international|general"
}"""


class SummarizerAgent:
    """
    Calls the configured LLM to generate headlines and 2-3 sentence summaries
    for each article in newsroom style.
    """

    AGENT_NAME = "summarizer"

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus      = bus or get_bus()
        self._db       = get_db()
        self._settings = get_settings()
        self._running  = False
        self._llm_calls = 0
        self._heartbeat_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running = True
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name=f"{self.AGENT_NAME}_heartbeat"
        )
        log.info(
            f"Summarizer started. LLM provider: {self._settings.llm_provider}",
            action="task_started",
        )

    async def stop(self) -> None:
        self._running = False
        if self._heartbeat_task:
            self._heartbeat_task.cancel()

    async def summarize_batch(self, articles: List[Article]) -> List[Article]:
        """
        Summarize all articles. Processes in parallel (max 5 concurrent LLM calls).
        Returns articles with headline and summary populated.
        """
        if not articles:
            return []

        semaphore = asyncio.Semaphore(5)
        results = await asyncio.gather(
            *[self._summarize_one(a, semaphore) for a in articles],
            return_exceptions=True,
        )

        summarized = []
        for article, result in zip(articles, results):
            if isinstance(result, Exception):
                log.error(f"Summarization failed for article {article.id}: {result}")
                # Keep article with original title as headline
                article.headline = article.title[:150]
                article.summary  = (article.raw_content or article.title)[:200]
                summarized.append(article)
            else:
                summarized.append(result)

        log.info(
            f"Summarization complete: {len(summarized)} articles",
            action="summary_generated",
            llm_calls=self._llm_calls,
        )

        await self._bus.publish(Event(
            Topics.ARTICLES_SUMMARIZED,
            payload={"count": len(summarized)},
            source=self.AGENT_NAME,
        ))
        return summarized

    async def _summarize_one(self, article: Article, sem: asyncio.Semaphore) -> Article:
        async with sem:
            try:
                result = await self._call_llm(article)
                article.headline = result.get("headline") or article.title[:150]
                article.summary  = result.get("summary")  or (article.raw_content[:200] if article.raw_content else article.title)
                # Update category if LLM suggests a better one
                suggested_cat = result.get("category", "").lower().replace(" ", "_")
                try:
                    article.category = Category(suggested_cat)
                except ValueError:
                    pass  # keep existing category
                self._llm_calls += 1
            except Exception as e:
                log.warning(f"LLM call failed, using fallback: {e}")
                article.headline = article.title[:150]
                article.summary  = (article.raw_content or article.title)[:200]
                # Keyword-based category fallback when LLM unavailable
                article.category = self._infer_category_from_keywords(article)
            return article

    def _infer_category_from_keywords(self, article: Article) -> Category:
        """Fallback category inference using keyword matching."""
        import json
        from pathlib import Path
        text = (article.title + " " + (article.raw_content or "")).lower()
        try:
            cats_data = json.loads(Path("config/categories.json").read_text()).get("categories", {})
        except Exception:
            return article.category

        best_cat = article.category
        best_score = 0
        for cat_name, cat_info in cats_data.items():
            keywords = cat_info.get("keywords", [])
            score = sum(1 for kw in keywords if kw in text)
            if score > best_score:
                best_score = score
                try:
                    best_cat = Category(cat_name)
                except ValueError:
                    pass
        return best_cat

    async def _call_llm(self, article: Article) -> dict:
        """Route to the correct LLM provider."""
        provider = self._settings.llm_provider
        user_msg = (
            f"Title: {article.title}\n"
            f"Content: {(article.raw_content or '')[:1500]}"
        )
        if provider == "gemini":
            return await self._call_gemini(user_msg)
        elif provider == "openai":
            return await self._call_openai(user_msg)
        elif provider == "groq":
            return await self._call_groq(user_msg)
        elif provider == "ollama":
            return await self._call_ollama(user_msg)
        else:
            raise ValueError(f"Unknown LLM provider: {provider}")

    # ── Provider implementations ──────────────────────────────

    async def _call_gemini(self, user_msg: str) -> dict:
        import google.generativeai as genai
        genai.configure(api_key=self._settings.gemini_api_key)
        model = genai.GenerativeModel(
            model_name=self._settings.gemini_model,
            system_instruction=SYSTEM_PROMPT,
        )
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: model.generate_content(user_msg)
        )
        return self._parse_json(response.text)

    async def _call_openai(self, user_msg: str) -> dict:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=self._settings.openai_api_key)
        resp = await client.chat.completions.create(
            model=self._settings.openai_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            temperature=0.2,
            max_tokens=300,
        )
        return self._parse_json(resp.choices[0].message.content)

    async def _call_groq(self, user_msg: str) -> dict:
        from groq import AsyncGroq
        client = AsyncGroq(api_key=self._settings.groq_api_key)
        resp = await client.chat.completions.create(
            model=self._settings.groq_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            temperature=0.2,
            max_tokens=300,
        )
        return self._parse_json(resp.choices[0].message.content)

    async def _call_ollama(self, user_msg: str) -> dict:
        import aiohttp
        url = f"{self._settings.ollama_base_url}/api/chat"
        payload = {
            "model": self._settings.ollama_model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": user_msg},
            ],
            "stream": False,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                data = await resp.json()
        return self._parse_json(data["message"]["content"])

    def _parse_json(self, text: str) -> dict:
        """Extract JSON from LLM response, tolerating markdown code blocks."""
        text = text.strip()
        # Strip ```json ... ``` wrapping
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to extract just the JSON object
            import re
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                return json.loads(match.group())
            raise ValueError(f"Could not parse LLM response as JSON: {text[:200]}")

    # ── Heartbeat ────────────────────────────────────────────

    async def _heartbeat_loop(self) -> None:
        while self._running:
            try:
                hb = AgentHeartbeat(
                    agent_name=self.AGENT_NAME,
                    status=AgentStatus.RUNNING,
                    metadata={"llm_calls": self._llm_calls, "provider": self._settings.llm_provider},
                )
                await self._db.upsert_heartbeat(hb.model_dump(mode="json"))
            except Exception as e:
                log.error(f"Heartbeat error: {e}")
            await asyncio.sleep(60)
