import os
import json
import logging
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv
import instructor
from openai import OpenAI, RateLimitError, APITimeoutError, APIConnectionError, APIError
from pydantic import BaseModel, Field, field_validator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from tqdm import tqdm

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ----- 1. Schema de Datos (Pydantic) -----

class AdminContact(BaseModel):
    """Un contacto individual extraído de la página."""
    nombre_completo: str = Field(description="Nombre completo de la persona. Si no se encuentra, usar 'NO_ENCONTRADO'")
    cargo: Optional[str] = Field(default=None, description="Cargo/rol en español tal como aparece: Administrador, Intendente, Gerente General, Presidente, etc.")
    email: Optional[str] = Field(default=None, description="Dirección de email si se encontró asociada a esta persona")
    telefono: Optional[str] = Field(default=None, description="Teléfono si se encontró asociado a esta persona")

    @field_validator('email')
    @classmethod
    def validate_email(cls, v):
        if v and not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', v.strip()):
            raise ValueError(f"Email inválido: {v}")
        return v.strip().lower() if v else None

    @field_validator('nombre_completo')
    @classmethod
    def validate_nombre(cls, v):
        if not v or v.strip() == '':
            raise ValueError("El nombre no puede estar vacío")
        return v.strip()

class ExtractionResult(BaseModel):
    """Resultado completo de la extracción para una página."""
    contactos: List[AdminContact] = Field(default_factory=list, description="Lista de contactos encontrados")
    emails_generales: List[str] = Field(default_factory=list, description="Emails que no están asociados a una persona específica")
    telefono_general: Optional[str] = Field(default=None, description="Teléfono general del barrio si existe")
    tiene_datos_contacto: bool = Field(description="True si se encontró CUALQUIER dato de contacto útil")

# ----- 2. Estrategia Híbrida: Regex + LLM -----

EMAIL_REGEX = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_REGEX = re.compile(r'(?:\+?54[\s-]?)?(?:11|[2368]\d)[\s-]?\d{4}[\s-]?\d{4}')

def pre_extract_regex(text: str) -> dict:
    """Extrae emails y teléfonos con regex antes de llamar al LLM."""
    return {
        "emails": list(set(EMAIL_REGEX.findall(text))),
        "phones": list(set(PHONE_REGEX.findall(text)))
    }

# ----- 3. Prompts del Sistema -----

SYSTEM_PROMPT = """You are a precise data extraction assistant specialized in Argentine gated communities (barrios cerrados, countries, barrios privados).

Your task: Extract ONLY official administrator/manager contact information from the text.

STRICT RULES TO AVOID INVALID LEADS:
1. IGNORE REAL ESTATE AGENCIES (INMOBILIARIAS): If a contact or email belongs to a real estate broker, sales agent, or agency (e.g., words like "Propiedades", "Inmobiliaria", "Realty", "Broker", "Ventas"), DISCARD IT. We only want the administration.
2. TARGET ROLES: Look for "Administración", "Intendencia", "Gerencia", "Secretaría", "Consejo de Propietarios", "Seguridad".
3. NO INVENTIONS: Extract ONLY what is explicitly in the text.
4. VALIDATION: If the text is just a house listing for sale, it likely won't have the neighborhood's admin contact. In that case, return NO_ENCONTRADO.
5. An email or phone not tied to a specific person goes in emails_generales or telefono_general if it looks like an official administration contact (e.g., admin@..., info@barrio...)."""

USER_PROMPT_TEMPLATE = """Extract administrator contact information from this Argentine gated community contact page:

---
{text}
---

Return structured JSON with the contacts found."""

# ----- 4. Sistema de Proveedores con Waterfall (cascada) -----

PROVIDERS = [
    {
        "name": "openrouter_paid",
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": os.getenv("OPENROUTER_API_KEY", ""),
        "model": "google/gemini-2.0-flash-001",
        "timeout": 60.0,
        "mode": instructor.Mode.JSON,
    },
    {
        "name": "openrouter_free",
        "base_url": "https://openrouter.ai/api/v1",
        "api_key": os.getenv("OPENROUTER_API_KEY", ""),
        "model": "meta-llama/llama-3.3-70b-instruct:free",
        "timeout": 60.0,
        "mode": instructor.Mode.MD_JSON,
    },
]

# ----- 5. Cliente con Reintentos (tenacity + instructor) -----

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((RateLimitError, APITimeoutError, APIConnectionError, APIError)),
    reraise=True
)
def llamada_llm_con_reintentos(client: instructor.Instructor, model: str, text: str) -> ExtractionResult:
    """Llama al LLM usando Instructor con reintentos para errores transitorios de red/rate limit."""
    return client.chat.completions.create(
        model=model,
        response_model=ExtractionResult,
        max_retries=2, # Instructor reintenta si la validación falla
        temperature=0.0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_PROMPT_TEMPLATE.format(text=text)},
        ]
    )

def ejecutar_extraccion_llm(text: str) -> tuple[Optional[ExtractionResult], str, Optional[str]]:
    """Intenta extraer datos usando los proveedores definidos en cascada (waterfall)."""
    for provider in PROVIDERS:
        if not provider["api_key"] and provider["name"] != "local_lmstudio":
            logger.warning(f"Proveedor {provider['name']} salteado: API Key no configurada.")
            continue
            
        try:
            logger.info(f"Intentando extracción con {provider['name']} ({provider['model']})")
            # Configurar cliente OpenAI básico
            base_client = OpenAI(
                base_url=provider["base_url"],
                api_key=provider["api_key"],
                timeout=provider["timeout"]
            )
            # Wrappear con instructor
            client = instructor.from_openai(base_client, mode=provider["mode"])
            
            # Llamada al LLM con retry de tenacity
            result = llamada_llm_con_reintentos(client, provider["model"], text)
            
            logger.info(f"Éxito con proveedor {provider['name']}")
            return result, provider["name"], None
            
        except Exception as e:
            logger.warning(f"Fallo con proveedor {provider['name']}: {str(e)}")
            continue
            
    # Si todos los LLMs fallaron
    logger.error("Todos los proveedores LLM fallaron para este registro.")
    return None, "regex_only", "Todos los LLM fallaron"

# ----- 8. Función de Health Check -----

def check_lmstudio_health() -> bool:
    """Verifica si LM Studio está corriendo y responde."""
    try:
        client = OpenAI(base_url="http://127.0.0.1:1234/v1", api_key="lm-studio", timeout=5.0)
        models = client.models.list()
        if len(models.data) > 0:
            logger.info("LM Studio online.")
            return True
        return False
    except Exception as e:
        logger.warning(f"LM Studio no está disponible u ocurrió un error: {str(e)}")
        return False

# ----- 6. Función Principal de Extracción -----

def extract_contacts(text: str, barrio_id: str, barrio_nombre: str) -> dict:
    """
    Extrae contactos de un texto de página web.
    
    Args:
        text: Texto limpio de la página de contacto (ya sin HTML tags)
        barrio_id: ID único del barrio para tracking
        barrio_nombre: Nombre del barrio para logging
    
    Returns:
        dict con keys: barrio_id, barrio_nombre, contactos, emails_regex, 
                       phones_regex, modelo_usado, error, timestamp
    """
    # 6.1. Ejecutar pre_extract_regex primero
    regex_data = pre_extract_regex(text)
    
    # 6.2. Truncar texto
    max_chars = 6000
    if len(text) > max_chars:
        logger.info(f"Truncando texto del barrio {barrio_id} ({len(text)} -> {max_chars} chars)")
        text_to_process = text[:max_chars]
    else:
        text_to_process = text
        
    # Inicializar resultado
    result_dict = {
        "barrio_id": barrio_id,
        "barrio_nombre": barrio_nombre,
        "contactos_llm": [],
        "emails_generales_llm": [],
        "telefono_general_llm": None,
        "emails_regex": regex_data["emails"],
        "phones_regex": regex_data["phones"],
        "modelo_usado": "regex_only",
        "error": None,
        "timestamp": datetime.now().isoformat()
    }
    
    if not text_to_process.strip():
        result_dict["error"] = "Texto vacío"
        return result_dict

    # 6.3. Intentar cascada de proveedores LLM
    llm_result, provider_name, error = ejecutar_extraccion_llm(text_to_process)
    
    result_dict["modelo_usado"] = provider_name
    
    if llm_result:
        # Serializar objetos Pydantic a dict
        result_dict["contactos_llm"] = [c.model_dump(exclude_none=True) for c in llm_result.contactos]
        result_dict["emails_generales_llm"] = llm_result.emails_generales
        result_dict["telefono_general_llm"] = llm_result.telefono_general
        result_dict["tiene_datos_contacto_llm"] = llm_result.tiene_datos_contacto
        
        # 6.4. Cruzar con emails de regex validando la consolidación final
        todos_los_emails = set(regex_data["emails"])
        for c in llm_result.contactos:
            if c.email: todos_los_emails.add(c.email)
        for e in llm_result.emails_generales:
            todos_los_emails.add(e)
            
        result_dict["todos_los_emails_unificados"] = list(todos_los_emails)

    if error:
        result_dict["error"] = error
        
    return result_dict

# ----- 7. Procesamiento en Batch con Resume -----

def run_extraction_batch(
    pages: list[dict], 
    output_path: str = "extraction_results.jsonl",
    delay_between_api_calls: float = 0.5,
    chunk_size: int = 100
) -> None:
    """Procesa un lote de páginas con el modelo de IA en chunks interactivos."""
    
    # 7.1. Leer JSONL existente para Resume
    procesados = set()
    if os.path.exists(output_path):
        with open(output_path, 'r', encoding='utf-8') as f:
            for n_line, line in enumerate(f):
                if line.strip():
                    try:
                        record = json.loads(line)
                        if "barrio_id" in record:
                            procesados.add(record["barrio_id"])
                    except json.JSONDecodeError:
                        logger.error(f"Línea corrupta en {output_path} (Línea {n_line+1})")
                        
    logger.info(f"Se encontraron {len(procesados)} registros procesados previamente. Serán ignorados.")
    
    # Filtrar páginas que ya fueron procesadas
    paginas_a_procesar = [p for p in pages if p.get("id") not in procesados]
    if not paginas_a_procesar:
        logger.info("No hay registros nuevos para procesar.")
        return
        
    total_pages = len(paginas_a_procesar)
    logger.info(f"Iniciando procesamiento de {total_pages} nuevas páginas.")
    
    # Crear chunks
    chunks = [paginas_a_procesar[i:i + chunk_size] for i in range(0, total_pages, chunk_size)]
    
    for i, chunk in enumerate(chunks):
        logger.info(f"\n--- Iniciando Lote {i+1} de {len(chunks)} ({len(chunk)} registros) ---")
        
        stat_exitos = 0
        stat_vacios = 0
        stat_errores = 0
        
        # Escribir archivo (append en modo jsonl)
        with open(output_path, 'a', encoding='utf-8') as f_out:
            for pagina in tqdm(chunk, desc=f"Lote {i+1}", unit="barrio"):
                barrio_id = pagina.get("id")
                barrio_nombre = pagina.get("nombre", "Desconocido")
                text = pagina.get("text", "")
                
                resultado = extract_contacts(text=text, barrio_id=barrio_id, barrio_nombre=barrio_nombre)
                
                # Estadísticas
                if resultado.get("error"):
                    stat_errores += 1
                elif resultado.get("tiene_datos_contacto_llm", False) or len(resultado.get("emails_regex", [])) > 0 or len(resultado.get("phones_regex", [])) > 0:
                    stat_exitos += 1
                else:
                    stat_vacios += 1
                
                # Flush on each iteration
                f_out.write(json.dumps(resultado, ensure_ascii=False) + "\n")
                f_out.flush()
                
                # Esperar para llamadas externas si es necesario
                if resultado["modelo_usado"] not in ["local_lmstudio", "regex_only"]:
                    time.sleep(delay_between_api_calls)
        
        # Reporte del chunk
        print("\n" + "="*50)
        print(f"📊 REPORTE DE EXTRACCIÓN - LOTE {i+1}/{len(chunks)}")
        print("="*50)
        print(f"🔹 Barrios esperados/procesados en este lote: {len(chunk)}")
        print(f"✅ Barrios con datos recolectados exitosamente: {stat_exitos}")
        print(f"⚠️ Barrios sin datos (vacíos): {stat_vacios}")
        print(f"❌ Barrios con error técnico: {stat_errores}")
        print("-" * 50)
        
        # Recomendaciones dinámicas
        print("💡 RECOMENDACIONES Y DIAGNÓSTICO:")
        
        if stat_errores > 0:
            print("   -> 🚨 Se detectaron errores. Motivación común: Tu internet falló, OpenRouter agotó los rate limits o la VRAM colapsó en LM Studio. Revisa la consola hacia arriba.")
        
        if stat_vacios > (len(chunk) * 0.4):
            print("   -> ⚠️ Alto volumen de barrios sin datos (más del 40%). Diagnóstico: Probablemente las páginas scrapeadas son solo 'Formularios de contacto' en blanco sin nombres ni mails escritos en texto. O la etiqueta HTML de Playwright está errando.")
        elif stat_vacios > 0:
            print("   -> ℹ️ Algunos barrios no arrojaron datos. Es normal; no todas las páginas web de barrios listan la información de la Intendencia de forma pública.")
            
        if stat_exitos > 0:
            print(f"   -> ✅ Extracción saludable. {stat_exitos} perfiles ricos en datos insertados exitosamente.")
            
        print("="*50)
        
        # Control de pausa/continuación
        if i < len(chunks) - 1:
            while True:
                resp = input(f"\n¿Deseas continuar procesando el siguiente lote de {len(chunks[i+1])} registros? (s/n): ").strip().lower()
                if resp in ['s', 'n']:
                    break
            
            if resp == 'n':
                print("\n⛔ Procesamiento por lotes pausado por el usuario.")
                print(f"Los datos descargados hasta ahora están seguros en '{output_path}'. El script reanudará desde aquí cuando lo vuelvas a ejecutar.")
                break
            else:
                print("\nContinuando con el siguiente lote...\n")

# ----- 9. Script de Test Rápido -----

if __name__ == "__main__":
    print("-" * 50)
    print("Iniciando Test Rápido de Extracción...")
    print("-" * 50)
    
    if check_lmstudio_health():
        print("✅ Health check: LM Studio está ONLINE.")
    else:
        print("⚠️ Health check: LM Studio está OFFLINE. Debería usar OpenRouter en caso de falla.")
        
    TEXTO_EJEMPLO = """
Barrio Privado Los Álamos - Contacto
Administración: Lic. Roberto Fernández
Email: administracion@losalamos.com.ar
Teléfono: (011) 4555-8900
Horario de atención: Lunes a Viernes de 9 a 17hs
Intendencia: Ing. María García
Email intendencia: intendencia@losalamos.com.ar

Para consultas comerciales: ventas@inmobiliarialopez.com.ar
"""
    print("\nProcesando el siguiente texto:")
    print(TEXTO_EJEMPLO)
    
    print("\nLlamando a extract_contacts()...")
    
    t0 = time.time()
    res = extract_contacts(TEXTO_EJEMPLO, barrio_id="test_001", barrio_nombre="Los Álamos (Test)")
    t1 = time.time()
    
    print(f"\n✅ Extracción finalizada en {t1 - t0:.2f} segundos.")
    print("Proveedor usado:", res["modelo_usado"])
    print("-" * 50)
    print("\nRESULTADO JSON:")
    print(json.dumps(res, indent=2, ensure_ascii=False))
