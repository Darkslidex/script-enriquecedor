import pandas as pd
import json
import time
import os
import re
from urllib.parse import quote, urlparse, parse_qs
from bs4 import BeautifulSoup
from curl_cffi import requests

# Importar tu excelente motor de IA
from llm_extractor import extract_contacts

# Dominios a ignorar durante la búsqueda (redes sociales genéricas o portales inmobiliarios)
BANNED_DOMAINS_SEARCH = [
    'facebook.com', 'instagram.com', 'zonaprop.com.ar', 'argenprop.com',
    'mercadolibre.com.ar', 'twitter.com', 'linkedin.com', 'tiktok.com',
    'youtube.com', 'pinterest.com', 'google.com', 'properati.com.ar',
    'remax.com.ar', 'mudafy.com.ar', 'foursquare.com', 'tripadvisor.com',
    'navent.com', 'infocasas.com', 'lamudi.com', 'bnpropiedades.com',
    'sidomus.com', 'roomix.ai', 'urbannext.net', 'todosnegocios.com',
    'eldia.com', 'infobae.com', 'clarin.com', 'lanacion.com.ar',
    'wikipedia.org', 'wikidata.org', 'yellowpages'
]

# Subpáginas de contacto/administración a intentar en orden de prioridad
CONTACT_SUBPAGES = [
    '/contacto', '/administracion', '/contactenos', '/contact',
    '/contacto/', '/administracion/', '/contactenos/', '/contact/',
    '/contacto.html', '/administracion.html', '/quienes-somos',
    '/la-administracion', '/consorcio', '/propietarios',
]

# --- 1. BUSCADORES WEB ---

def ask_ddg(query):
    url = f"https://html.duckduckgo.com/html/?q={quote(query)}"
    try:
        r = requests.get(url, impersonate="chrome120", timeout=15)
        if r.status_code == 429 or 'Just a moment' in r.text.lower() or 'cloudflare' in r.text.lower():
            return None # Bloqueado, requiere fallback
            
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for a in soup.find_all('a', class_='result__snippet', href=True):
            href = a['href']
            if href.startswith('//duckduckgo.com/l/?'):
                parsed = urlparse(href)
                qs = parse_qs(parsed.query)
                real_url = qs.get('uddg', [''])[0]
                if real_url: links.append(real_url)
                
        valid_links = []
        for l in links:
            domain = urlparse(l).netloc.lower()
            if not any(b in domain for b in BANNED_DOMAINS_SEARCH):
                valid_links.append(l)
                
        return valid_links
    except Exception:
        return []

def ask_bing(query):
    url = f"https://www.bing.com/search?q={quote(query)}"
    try:
        r = requests.get(url, impersonate="chrome120", timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        links = []
        for cite in soup.find_all('cite'):
            url_text = cite.text.strip()
            if url_text.startswith('http'):
                 links.append(url_text)
            else:
                 links.append('https://' + url_text)
                 
        valid_links = []
        for l in links:
            domain = urlparse(l).netloc.lower()
            if not any(b in domain for b in BANNED_DOMAINS_SEARCH):
                valid_links.append(l)
        return valid_links
    except:
        return []

def buscar_sitio_web(nombre_barrio: str, partido: str) -> str:
    """Intenta buscar la web oficial de un barrio con múltiples queries progresivas."""
    # Secuencia de queries de más a menos específica, priorizando Argentina
    queries = [
        f'"{nombre_barrio}" administracion contacto sitio:com.ar',
        f'"{nombre_barrio}" {partido} administracion email contacto',
        f'"{nombre_barrio}" barrio privado intendencia email Argentina',
        f'"{nombre_barrio}" barrio cerrado contacto',
    ]

    for query in queries:
        links = ask_ddg(query)
        if links is None:  # DDG bloqueado, fallback a Bing
            time.sleep(1.5)
            links = ask_bing(query)

        if links:
            # Preferir dominios .com.ar sobre otros
            ar_links = [l for l in links if '.com.ar' in urlparse(l).netloc or '.org.ar' in urlparse(l).netloc]
            return ar_links[0] if ar_links else links[0]

        time.sleep(0.5)  # Pausa entre queries para no saturar

    return ""

# --- 2. DESCARGADOR DE SITIOS ---

def _descargar_pagina(url: str) -> str:
    """Descarga una URL y retorna el texto limpio (sin HTML)."""
    try:
        r = requests.get(url, impersonate="chrome120", timeout=12, verify=False)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, 'html.parser')
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.extract()
        return soup.get_text(separator=' ', strip=True)
    except Exception:
        return ""

def obtener_texto_web(url: str) -> str:
    """Descarga el sitio principal + subpáginas de contacto/administración.
    Combina el texto de todas las páginas para maximizar la detección de emails.
    """
    if not url:
        return ""

    # Parsear base URL (esquema + dominio sin path)
    parsed = urlparse(url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    # 1. Descargar la página principal
    texto_principal = _descargar_pagina(url)

    # 2. Intentar subpáginas de contacto/administración
    textos_sub = []
    for subpage in CONTACT_SUBPAGES:
        sub_url = base_url + subpage
        if sub_url == url:  # Evitar re-descargar la misma página
            continue
        texto_sub = _descargar_pagina(sub_url)
        if texto_sub:
            textos_sub.append(texto_sub)
            break  # Con una subpágina exitosa es suficiente

    # 3. Combinar texto (principal + subpágina de contacto)
    partes = [texto_principal] + textos_sub
    texto_combinado = ' | '.join(p for p in partes if p)

    # Limitar a 12000 chars para no saturar el contexto de la IA
    return texto_combinado[:12000]

# --- 3. FLUJO PRINCIPAL ---

def main():
    print("="*60)
    print("🤖 INICIANDO PIPELINE INTEGRADO: BÚSQUEDA + IA 🤖")
    print("="*60)
    
    archivo_csv = "barrios_enriquecidos_completo.csv"
    archivo_salida_ia = "extraction_results_fase1_ia.jsonl"
    
    df = pd.read_csv(archivo_csv)
    
    # Cargar progreso previo para permitir reanudar
    procesados = set()
    if os.path.exists(archivo_salida_ia):
        with open(archivo_salida_ia, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        if "barrio_id" in record:
                            procesados.add(record["barrio_id"])
                    except:
                        pass
                        
    # Identificar pendientes. Filtramos todo lo que aún no testeamos por IA
    df_pendientes = df[~df.index.astype(str).isin(procesados)]
    
    print(f"Total de registros a procesar: {len(df_pendientes)}")
    if df_pendientes.empty:
        print("¡Todos los registros ya han sido procesados por el nuevo pipeline!")
        return

    # Proceso interactivo de lotes (batching de a 50)
    lote_actual = 0
    LOTE_SIZE = 50
    
    for idx, row in df_pendientes.iterrows():
        barrio_id = str(idx)
        nombre = str(row['nombre'])
        partido = str(row['partido']) if pd.notna(row['partido']) else ""
        url_actual = str(row['sitio_web']) if pd.notna(row['sitio_web']) else ""
        
        print(f"\n({lote_actual+1}/{LOTE_SIZE}) Barrio ID {barrio_id}: {nombre} ({partido})")
        
        # 1. Búsqueda si no hay sitio web
        if not url_actual or url_actual.strip() == "":
            print(f"  🔍 Sin sitio web. Buscando en interet...")
            url_encontrada = buscar_sitio_web(nombre, partido)
            if url_encontrada:
                print(f"  🌐 Posible sitio web encontrado: {url_encontrada}")
                url_actual = url_encontrada
            else:
                print(f"  ❌ No se encontró un sitio web claro en buscadores.")
        else:
            print(f"  🔗 Usando sitio web del CSV: {url_actual}")
        
        # 2. Descargar y procesar texto
        texto_scraped = ""
        if url_actual:
            print("  📖 Descargando contenido del sitio...")
            texto_scraped = obtener_texto_web(url_actual)
            
        # 3. Pasar por el extractor de IA (o guardar como fallido si no había texto)
        record = {
            "barrio_id": barrio_id,
            "barrio_nombre": nombre,
            "url_usada": url_actual
        }
        
        if texto_scraped:
            print("  🤖 Analizando texto con Gemini Flash...")
            ia_result = extract_contacts(texto_scraped, barrio_id, nombre)
            record.update(ia_result) # Fusiona los emails detectados
            
            # Un reporte cortito en consola:
            correos_consolidados = ia_result.get("todos_los_emails_unificados", [])
            if correos_consolidados:
                print(f"  ✅ ¡IA Encontró contactos!: {correos_consolidados}")
            else:
                print(f"  ⚠️ La IA no encontró emails oficiales de la administración aquí.")
        else:
            record["error"] = "Sin web / Texto vacío"
            record["todos_los_emails_unificados"] = []
            
        # 4. Guardar archivo inmediatamente (safety feature)
        with open(archivo_salida_ia, 'a', encoding='utf-8') as f_out:
            f_out.write(json.dumps(record, ensure_ascii=False) + "\n")
            f_out.flush()
            
        lote_actual += 1
        
        # 5. Pausa por lote
        if lote_actual >= LOTE_SIZE:
            print("\n" + "="*50)
            print("🛑 LIMITE DE LOTE ALCANZADO 🛑")
            print("El progreso parcial se ha guardado de forma segura.")
            
            while True:
                resp = input(f"¿Deseas descargar y procesar los siguientes {LOTE_SIZE} barrios? (s/n): ").strip().lower()
                if resp in ['s', 'n']:
                    break
                    
            if resp == 'n':
                print("\n⛔ Pipeline pausado por el usuario. Nos vemos pronto.")
                return
            else:
                lote_actual = 0
                
    print("\n🏁 Procesamiento total completado al 100%.")

if __name__ == "__main__":
    main()
