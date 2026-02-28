import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# --- CONFIGURACIÓN ---
ACCESS_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJOaWNvbGFzIERpYXoiLCJidXNpbmVzc0lkIjoiZ291dCIsIm5hbWUiOiJOaWNvbGFzIERpYXoiLCJhcGkiOnRydWUsImlkIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiIsImV4cCI6MTkyNTY4MTk0NywianRpIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiJ9.IaLL89IOdyOH3hhmll2KTvXKU9mmW-n7JHpv7O1M7YHXrwloMhB1AYzIM4FKlKh5J_ueTQacUYAe32ZAfq9-Dw"
URL = "https://api.botmaker.com/v2.0/sessions"

START_DATE = datetime(2026, 1, 2, 0, 0, 0)
END_DATE = datetime(2026, 1, 2, 23, 59, 59)
HOURS_STEP = 1

def extraer_todo_botmaker():
    data_total = []
    current_start = START_DATE

    with requests.Session() as session:
        session.headers.update({"Accept": "application/json", "access-token": ACCESS_TOKEN})

        while current_start < END_DATE:
            current_end = current_start + timedelta(hours=HOURS_STEP)
            if current_end > END_DATE: current_end = END_DATE

            params = {
                "from": current_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "to": current_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "long-term-search": "true",
                "include-variables": "true",
                "include-ai-analysis": "true",
                "include-events": "true",
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
                    data_total.extend(items)
                    print(f"⏳ {current_start.strftime('%H:%M')} | Registros acumulados: {len(data_total)}", end="\r")
                    page_token = res_json.get("nextPage")
                    if not page_token: break
                except Exception as e:
                    print(f"\n❌ Error: {e}. Reintentando...")
                    time.sleep(10)
                    continue
            current_start = current_end
    return data_total

# --- EJECUCIÓN ---
raw_data = extraer_todo_botmaker()

if raw_data:
    print("\n\n📦 Procesando datos generales...")

    # Expandir los datos JSON a DataFrame
    df = pd.json_normalize(raw_data, sep='_')

    # 1. Limpieza de duplicados por el ID único de Botmaker
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'])

    # 2. LIMPIEZA DE NOMBRES Y FORMATOS
    # Limpieza estética de nombres de columnas
    df.columns = [c.replace('chat_', '').replace('variables_', 'VAR_') for c in df.columns]

    # Convertir fechas a formato legible (sin zona horaria para Excel)
    fechas_cols = [c for c in df.columns if any(x in c for x in ['Time', 'Date', 'inicio', 'creationTime'])]
    for col in fechas_cols:
        try:
            df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
        except: pass

    # 3. GUARDAR EXCEL
    filename = f"REPORTE_GENERAL_{START_DATE.strftime('%Y%m%d')}.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')

    print(f"🏁 ¡LISTO! Reporte generado con {len(df)} registros totales.")
    print(f"💾 Archivo: {filename}")
else:
    print("\nNo se obtuvieron datos de la API.")