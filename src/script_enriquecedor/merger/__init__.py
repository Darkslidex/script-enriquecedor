"""Módulo unificador de fuentes scraper + LinkedIn Sales Navigator.

Flujo:
  [S] Scrapear → data/output/scraper_{rubro}_{fecha}.csv
  (en paralelo) → operador copia CSV de Sales Navigator en data/input/linkedin/
  [M] Merge    → merger/unifier.py lee ambas fuentes → data/output/unified_{rubro}_{fecha}.csv
  [U] Upload   → rsync unified_{rubro}.csv → VPS → seed.ts
"""
