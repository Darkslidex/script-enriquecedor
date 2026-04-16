"""Fetcher HTTP async.

- httpx async como cliente principal
- Fallback automático a Playwright cuando recibe 403 o el sitio es JS-heavy
- Respeto de robots.txt (via robots.py)
- Rate limiting por dominio (via rate_limiter.py)
- Pool de user agents rotativo (via user_agents.py)
"""

# TODO: implementar (Fase 1 paso 7)
