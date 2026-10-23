"""Structured output of an investigation."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class Claim(BaseModel):
    """One factual statement in the narrative, tied to the tool call it came from."""

    statement: str = Field(description="The claim as written in the narrative.")
    value: str = Field(description="The exact number the claim rests on, copied from the tool output (e.g. '56833.67').")
    tool_call_id: str = Field(description="The id of the tool call whose output contains this number.")


class Verdict(BaseModel):
    classification: Literal["likely_benign", "escalate"] = Field(
        description="escalate if the evidence is consistent with the manipulation pattern; likely_benign otherwise.")
    confidence: Literal["low", "medium", "high"]
    hypothesis: str = Field(description="One sentence: what most plausibly happened.")
    narrative: str = Field(description="A short case narrative (4-8 sentences) for a surveillance analyst.")
    claims: list[Claim] = Field(description="Every number used in the narrative, with its source tool call.")
    benign_explanations_considered: list[str] = Field(
        description="Ordinary market explanations checked, and why each does or does not fit.")
