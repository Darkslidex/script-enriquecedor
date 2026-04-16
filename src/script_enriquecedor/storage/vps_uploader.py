"""Upload al VPS via rsync + seed.ts remoto.

Paso 1: rsync data/enriched/<vertical>/consolidated.csv → bunker:/root/apps/barrios-dashboard/data/
Paso 2: ssh bunker "docker run ... npx tsx prisma/seed.ts"

El seed es idempotente. Esta clase solo ejecuta los comandos, no duplica la lógica de dedup.
"""

# TODO: implementar (Fase 1 paso 9)
