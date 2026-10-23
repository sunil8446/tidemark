"""Token usage and dollar cost per investigation."""

from __future__ import annotations

from dataclasses import dataclass, field

# USD per million tokens, Claude API first-party rates (claude-api skill table, cached 2026-10-06).
# Cache writes (5-minute TTL) cost 1.25x input.
PRICES = {
    "claude-opus-5-5": {"input": 4.00, "output": 20.00, "cache_read": 0.20},
    "claude-sonnet-5-5": {"input": 2.00, "output": 10.00, "cache_read": 0.20},
    "claude-haiku-5-5": {"input": 0.10, "output": 0.50, "cache_read": 0.01},
}


@dataclass
class Usage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_input_tokens: int = 0
    cache_creation_input_tokens: int = 0
    requests: int = 0
    by_model: dict[str, dict[str, int]] = field(default_factory=dict)

    def add(self, model: str, usage) -> None:
        def g(name):
            return getattr(usage, name, 0) or 0

        vals = {
            "input_tokens": g("input_tokens"),
            "output_tokens": g("output_tokens"),
            "cache_read_input_tokens": g("cache_read_input_tokens"),
            "cache_creation_input_tokens": g("cache_creation_input_tokens"),
        }
        for k, v in vals.items():
            setattr(self, k, getattr(self, k) + v)
            self.by_model.setdefault(model, {}).setdefault(k, 0)
            self.by_model[model][k] += v
        self.requests += 1

    def cost_usd(self) -> float:
        total = 0.0
        for model, u in self.by_model.items():
            p = PRICES.get(model, PRICES["claude-opus-5-5"])
            total += (
                u["input_tokens"] * p["input"]
                + u["cache_creation_input_tokens"] * p["input"] * 1.25
                + u["cache_read_input_tokens"] * p["cache_read"]
                + u["output_tokens"] * p["output"]
            ) / 1_000_000
        return round(total, 6)

    def as_dict(self) -> dict:
        return {
            "requests": self.requests,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cache_read_input_tokens": self.cache_read_input_tokens,
            "cache_creation_input_tokens": self.cache_creation_input_tokens,
            "cost_usd": self.cost_usd(),
        }
