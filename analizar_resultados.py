import json
import pandas as pd

# ---- Cargar resultados del pipeline nuevo ----
with open('extraction_results_fase1_ia.jsonl', 'r', encoding='utf-8') as f:
    lines = [json.loads(l) for l in f if l.strip()]

total = len(lines)
sin_web = [r for r in lines if r.get('error') == 'Sin web / Texto vacío']
con_url = [r for r in lines if r.get('url_usada', '').strip()]
llm_ok = [r for r in lines if r.get('tiene_datos_contacto_llm') == True]
email_admin = [r for r in lines if r.get('tiene_datos_contacto_llm') and r.get('todos_los_emails_unificados')]
solo_regex = [r for r in lines if not r.get('tiene_datos_contacto_llm') and r.get('todos_los_emails_unificados')]

# ---- Cargar CSV original para cruzar qué ya teníamos ----
df_orig = pd.read_csv('barrios_enriquecidos_completo.csv')
emails_ya_existentes = set()
if 'email' in df_orig.columns:
    emails_ya_existentes = set(df_orig['email'].dropna().str.lower().tolist())

# ---- Emails nuevos generados por este pipeline ----
todos_emails_nuevos = []
for r in lines:
    for e in r.get('todos_los_emails_unificados', []):
        if e.lower() not in emails_ya_existentes:
            todos_emails_nuevos.append((r['barrio_nombre'], e, r.get('modelo_usado',''), r.get('tiene_datos_contacto_llm', False)))

# ---- Timestamps para ver velocidad de lotes ----
timestamps = []
for r in lines:
    if r.get('timestamp'):
        timestamps.append(r['timestamp'])

print("=" * 60)
print("📊 REPORTE COMPLETO DEL PIPELINE")
print("=" * 60)
print(f"  Total registros procesados : {total}")
print(f"  Sin web encontrada         : {len(sin_web)} ({100*len(sin_web)//total}%)")
print(f"  Con URL visitada           : {len(con_url)} ({100*len(con_url)//total}%)")
print(f"  IA encontró datos          : {len(llm_ok)}")
print(f"  Leads admin con email (IA) : {len(email_admin)}")
print(f"  Solo regex (sin confirm IA): {len(solo_regex)}")
print()

print("=" * 60)
print(f"🆕 EMAILS NUEVOS (no estaban en CSV original) : {len(todos_emails_nuevos)}")
print("=" * 60)
nuevos_admin = [(b, e, m) for b, e, m, ia in todos_emails_nuevos if ia]
nuevos_regex = [(b, e, m) for b, e, m, ia in todos_emails_nuevos if not ia]

if nuevos_admin:
    print(f"\n✅ EMAILS ADMIN CONFIRMADOS POR IA ({len(nuevos_admin)}):")
    for barrio, email, modelo in nuevos_admin:
        print(f"  {barrio}: {email}")
else:
    print("\n  (ninguno con confirmación IA aún)")

if nuevos_regex:
    print(f"\n⚠️  EMAILS ENCONTRADOS SOLO POR REGEX (pueden ser inmobiliarias) ({len(nuevos_regex)}):")
    for barrio, email, modelo in nuevos_regex:
        print(f"  {barrio}: {email}")

print()
print("=" * 60)
print("📋 DETALLE COMPLETO CON TELÉFONOS:")
print("=" * 60)
for r in lines:
    if r.get('tiene_datos_contacto_llm') or r.get('todos_los_emails_unificados'):
        print(f"\n  🏘️  {r['barrio_nombre']}")
        print(f"     URL: {r.get('url_usada','')[:80]}")
        if r.get('contactos_llm'):
            for c in r['contactos_llm']:
                if c.get('nombre_completo') != 'NO_ENCONTRADO':
                    print(f"     👤 {c.get('nombre_completo')} ({c.get('cargo','')})")
        if r.get('emails_generales_llm'):
            print(f"     📧 {r['emails_generales_llm']}")
        if r.get('telefono_general_llm'):
            print(f"     📞 {r['telefono_general_llm']}")
        if r.get('emails_regex') and not r.get('tiene_datos_contacto_llm'):
            print(f"     🔍 [REGEX ONLY] {r['emails_regex']}")

if timestamps:
    print()
    print(f"🕐 Primer registro: {timestamps[0]}")
    print(f"🕐 Último registro : {timestamps[-1]}")
