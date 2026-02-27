import requests
import pandas as pd
from datetime import datetime, timedelta
import time # Importamos la librería para el tiempo

# 1. Configuración
url = "https://api.botmaker.com/v2.0/dashboards/agent-metrics"
token = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJOaWNvbGFzIERpYXoiLCJidXNpbmVzc0lkIjoiZ291dCIsIm5hbWUiOiJOaWNvbGFzIERpYXoiLCJhcGkiOnRydWUsImlkIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiIsImV4cCI6MTkyNTY4MTk0NywianRpIjoib1RiV1NwNlFnM1hCZmN0MkhIS2liQTFES0YxMiJ9.IaLL89IOdyOH3hhmll2KTvXKU9mmW-n7JHpv7O1M7YHXrwloMhB1AYzIM4FKlKh5J_ueTQacUYAe32ZAfq9-Dw"

headers = {
    "Accept": "application/json",
    "access-token": token.strip()
}

# --- INICIO DEL TEMPORIZADOR ---
inicio_proceso = time.time()

# Rango de fechas (Febrero 2026)
fecha_inicio = datetime(2026, 2, 1)
fecha_fin = datetime(2026, 2, 28)

lista_dfs_uruguay = []

def filtrar_uruguay(x):
    if isinstance(x, list):
        return any('ColaUruguay' in str(item) for item in x)
    return 'ColaUruguay' in str(x)

print(f"🚀 Iniciando descarga de datos desde {fecha_inicio.date()} hasta {fecha_fin.date()}...")
print("-" * 50)

# 2. Ejecución del bucle
fecha_actual = fecha_inicio
while fecha_actual <= fecha_fin:
    dia_str = fecha_actual.strftime("%Y-%m-%d")
    print(f"Consultando día: {dia_str}...", end=" ", flush=True)

    querystring = {
        "session-status": "closed",
        "from": f"{dia_str}T00:00:00.000Z",
        "to": f"{dia_str}T23:59:59.999Z",
    }

    try:
        response = requests.get(url, headers=headers, params=querystring, timeout=60)

        if response.status_code == 200:
            data = response.json()
            if "items" in data and data["items"]:
                df_dia = pd.json_normalize(data["items"])
                
                if 'queue' in df_dia.columns:
                    df_filtro = df_dia[df_dia['queue'].apply(filtrar_uruguay)].copy()
                    if not df_filtro.empty:
                        lista_dfs_uruguay.append(df_filtro)
                        print(f"✅ {len(df_filtro)} registros encontrados.")
                    else:
                        print("Empty (No Uruguay).")
                else:
                    print("⚠️ No 'queue' column.")
            else:
                print("ℹ️ Sin datos.")
        else:
            print(f"❌ Error {response.status_code}")
            if response.status_code == 401:
                print("🛑 Token expirado. Deteniendo proceso.")
                break
    except Exception as e:
        print(f"❌ Error de conexión: {e}")

    fecha_actual += timedelta(days=1)

# 3. Consolidación Final y Tiempos
if lista_dfs_uruguay:
    print("\n" + "="*50)
    print("📦 Generando archivo Excel consolidado...")
    df_final = pd.concat(lista_dfs_uruguay, ignore_index=True)
    
    # Limpieza de fechas para Excel
    cols_fecha = ['sessionCreationTime', 'closedTime']
    for col in cols_fecha:
        if col in df_final.columns:
            df_final[col] = pd.to_datetime(df_final[col], format='ISO8601', errors='coerce').dt.tz_localize(None)

    nombre_archivo = "REPORTE_URUGUAY_FEBRERO_COMPLETO.xlsx"
    df_final.to_excel(nombre_archivo, index=False, engine='openpyxl')
    
    # --- CÁLCULO DE TIEMPO FINAL ---
    fin_proceso = time.time()
    tiempo_total = fin_proceso - inicio_proceso
    minutos = int(tiempo_total // 60)
    segundos = int(tiempo_total % 60)

    print(f"✅ ¡LISTO! Archivo creado: {nombre_archivo}")
    print(f"📊 Total registros de Uruguay: {len(df_final)}")
    print(f"⏱️ Tiempo total de ejecución: {minutos} min {segundos} seg")
    print("="*50)
else:
    print("\n❌ No se recolectaron datos en el periodo seleccionado.")