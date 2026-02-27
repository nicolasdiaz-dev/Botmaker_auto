import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# --- 1. CONFIGURACIÓN ---
ACCESS_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJOaWNvbGFzIERpYXoiLCJidXNpbmVzc0lkIjoiZ291dCIsIm5hbWUiOiJOaWNvbGFzIERpYXoiLCJhcGkiOnRydWUsImlkIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiIsImV4cCI6MTkyNTY4MTk0NywianRpIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiJ9.IaLL89IOdyOH3hhmll2KTvXKU9mmW-n7JHpv7O1M7YHXrwloMhB1AYzIM4FKlKh5J_ueTQacUYAe32ZAfq9-Dw"
URL = "https://api.botmaker.com/v2.0/sessions"

# Rango de fechas
START_DATE = datetime(2026, 2, 2, 0, 0, 0)
END_DATE = datetime(2026, 2, 2, 23, 59, 59)
HOURS_STEP = 2 # Aumentamos a 2 horas para ir más rápido si hay pocos datos

def extraer_uruguay_botmaker():
    data_total = []
    current_start = START_DATE
    
    print(f"🚀 Extrayendo ColaUruguay: {START_DATE.date()}...")
    print("-" * 50)

    with requests.Session() as session:
        session.headers.update({"Accept": "application/json", "access-token": ACCESS_TOKEN})

        while current_start < END_DATE:
            current_end = min(current_start + timedelta(hours=HOURS_STEP), END_DATE)

            params = {
                "from": current_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "long-term-search": "true",
                "include-variables": "false", # Falso para máxima velocidad (mínimas columnas)
                "include-events": "false",    # Falso para no saturar la memoria
                "include-messages": "false"
            }

            page_token = None
            while True:
                if page_token: params["page"] = page_token
                try:
                    response = session.get(URL, params=params, timeout=120)
                    response.raise_for_status()
                    res_json = response.json()

                    items = res_json.get("items", [])
                    
                    # FILTRO INMEDIATO: Solo ColaUruguay
                    for item in items:
                        queues = item.get("queue", [])
                        # Verificamos si 'ColaUruguay' está en la lista de colas de la sesión
                        if any("ColaUruguay" in str(q) for q in queues if q):
                            data_total.append(item)

                    print(f"⏳ {current_start.strftime('%H:%M')} | Uruguay encontrados: {len(data_total)}", end="\r")

                    page_token = res_json.get("nextPage")
                    if not page_token: break
                except Exception as e:
                    print(f"\n❌ Error: {e}. Reintentando...")
                    time.sleep(5)
                    continue

            current_start = current_end

    return data_total

# --- 2. EJECUCIÓN ---
inicio_timer = time.time()
raw_data = extraer_uruguay_botmaker()

if raw_data:
    print("\n\n📦 Generando reporte mínimo...")
    df = pd.json_normalize(raw_data)

    # COLUMNAS MÍNIMAS INDISPENSABLES
    columnas_map = {
        'creationTime': 'Fecha_Temp',
        'id': 'ID Sesion',
        'chat_contactId': 'WhatsApp/ID',
        'operator_name': 'Agente',
        'chat_lastTag': 'Ultima Tag'
    }

    # Seleccionar solo lo necesario
    df = df[list(columnas_map.keys())].rename(columns=columnas_map)

    # Formatear Fecha
    df['Fecha_Temp'] = pd.to_datetime(df['Fecha_Temp'])
    df.insert(0, 'Fecha', df['Fecha_Temp'].dt.date)
    df.insert(1, 'Hora', df['Fecha_Temp'].dt.strftime('%H:%M'))
    df.drop(columns=['Fecha_Temp'], inplace=True)

    # Guardar
    nombre_archivo = "REPORTE_URUGUAY_MINIMO.xlsx"
    df.to_excel(nombre_archivo, index=False, engine='openpyxl')
    
    print(f"✅ LISTO: {len(df)} sesiones de Uruguay guardadas.")
    print(f"⏱️ Tiempo: {round((time.time() - inicio_timer), 2)} seg.")
else:
    print("\n❌ No se encontraron datos para Uruguay en este rango.")