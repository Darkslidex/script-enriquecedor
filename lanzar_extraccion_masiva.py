import pandas as pd
from curl_cffi import requests
from bs4 import BeautifulSoup
import time
import os
import json
from llm_extractor import run_extraction_batch

def obtener_texto_web(url: str) -> str:
    """Visita una URL web y extrae todo su texto en crudo para que la IA lo lea."""
    try:
        # Usamos curl_cffi (Chrome 120) para saltarnos los bloqueos antibot igual que antes
        r = requests.get(url, impersonate="chrome120", timeout=15, verify=False)
        if r.status_code != 200:
            return ""
            
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Eliminar el código basura (JavaScript y Estilos) para no marear a la IA
        for script in soup(["script", "style", "nav", "footer"]): 
            script.extract()
            
        texto_limpio = soup.get_text(separator=' ', strip=True)
        return texto_limpio
    except Exception as e:
        return ""

def main():
    print("="*60)
    print("🚀 INICIANDO EXTRACCIÓN MASIVA CON LLM 🚀")
    print("="*60)

    # 1. Cargar la base de datos que tu Scraper V3 construyó
    csv_file = "barrios_enriquecidos_completo.csv"
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"❌ Error: No se encontró el archivo {csv_file}")
        return

    # 2. Filtrar barrios que SÍ tengan sitio web oficial
    # Aquí puedes incluir también páginas de Facebook si la IA te las lee bien.
    df_con_web = df[df['sitio_web'].notna() & (df['sitio_web'] != "")]
    
    print(f"✅ Se encontraron {len(df_con_web)} barrios listos para ser analizados por la IA.")

    # 3. Leer progreso previo para no volver a descargar sitios ya procesados
    procesados = set()
    output_path = "extraction_results.jsonl"
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        if "barrio_id" in record:
                            procesados.add(record["barrio_id"])
                    except:
                        pass
    
    if len(procesados) > 0:
        print(f"🔄 Detectados {len(procesados)} barrios ya procesados. Saltando descargas redundantes...")

    # 4. Descargar el texto de cada sitio web en demanda y empaquetar
    paginas_preparadas = []
    
    print("🌐 Descargando y leyendo contenido de los sitios webs nuevos...")
    
    # IMPORTANTE: Aquí filtramos los que YA están procesados antes de empezar la descarga
    df_pendientes = df_con_web[~df_con_web.index.astype(str).isin(procesados)]
    
    if df_pendientes.empty:
        print("✅ ¡Todos los barrios con web ya fueron procesados!")
        return

    print(f"⏳ Pendientes de descargar/analizar: {len(df_pendientes)} barrios.")

    for index, row in df_pendientes.iterrows():
        url = row['sitio_web']
        nombre_barrio = str(row['nombre'])
        barrio_id = str(row.name)
        
        # Opcional: Para evitar scrapear Facebooks gigantes
        if "facebook" in url.lower():
            continue

        print(f"🔍 [{len(paginas_preparadas)+1}/{len(df_pendientes)}] Extrayendo texto de: {nombre_barrio} ({url})")
        texto_scraped = obtener_texto_web(url)
        
        paginas_preparadas.append({
            "id": barrio_id,
            "nombre": nombre_barrio,
            "text": texto_scraped
        })
        
        # Cada 100 descargas procesamos el lote y pedimos confirmación
        if len(paginas_preparadas) >= 100:
            print(f"\n⚡ Procesando lote intermedio de 100 para asegurar progreso...")
            run_extraction_batch(
                pages=paginas_preparadas,
                output_path=output_path,
                delay_between_api_calls=1.0,
                chunk_size=100
            )
            paginas_preparadas = [] # Limpiar para el siguiente lote
            
            while True:
                resp = input("\n¿Deseas descargar y procesar los siguientes 100 barrios? (s/n): ").strip().lower()
                if resp in ['s', 'n']:
                    break
            if resp == 'n':
                print("\n⛔ Proceso pausado por tu orden. Puedes volver a ejecutar el script en el futuro.")
                return

        time.sleep(0.3) 

    # 5. Lanzar el remanente si quedó algo en la lista
    if paginas_preparadas:
        print("\n🧠 Procesando el último lote de textos...\n")
        run_extraction_batch(
            pages=paginas_preparadas,
            output_path=output_path,
            delay_between_api_calls=1.0,
            chunk_size=100
        )


    print("\n🏁 Proceso Maestro Finalizado.")

if __name__ == "__main__":
    main()
