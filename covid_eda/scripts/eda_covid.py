from dagster import asset, AssetCheckSpec, AssetCheckResult, Output
import pandas as pd
import requests
from io import StringIO
from datetime import datetime

# URL de los datos OWID
CSV_URL = "https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv"

# Países a comparar
PAISES = ["Ecuador", "Peru"]


# Asset 1: Lectura de Datos

@asset(description="Leer CSV remoto de OWID y retornar DataFrame sin limpiar.")
def leer_datos() -> pd.DataFrame:
    # Hacer una petición HTTP GET al CSV remoto
    response = requests.get(CSV_URL)
    
    # Verificar que la descarga fue exitosa; si no, lanzar excepción
    response.raise_for_status()
    
    # Leer el contenido CSV en un DataFrame de pandas
    df = pd.read_csv(StringIO(response.text))

    # Normalizar la columna de país:
    # Si existe la columna "country", renombrarla a "location"
    if "country" in df.columns:
        df = df.rename(columns={"country": "location"})
    # Si no existe ni "country" ni "location", lanzar un error
    elif "location" not in df.columns:
        raise KeyError("No se encontró ni 'country' ni 'location' en el CSV.")

    # Devolver el DataFrame completo sin limpieza adicional
    return df



# -----------------------------
# Asset 2: Limpieza de Datos
# -----------------------------
@asset(description="Limpieza de datos: fechas válidas, nulos manejados y consistencia básica.")
def datos_limpios(leer_datos: pd.DataFrame) -> pd.DataFrame:
    # Crear una copia del DataFrame original para no modificarlo directamente
    df = leer_datos.copy()

    # Convertir la columna "date" a tipo datetime
    # Si alguna fecha no puede convertirse, se marca como NaT (valor nulo)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Eliminar todas las filas donde la fecha es inválida o nula
    df = df.dropna(subset=["date"])

    # Obtener la fecha actual y filtrar filas con fechas mayores (futuras)
    hoy = pd.to_datetime(datetime.today().date())
    df = df[df["date"] <= hoy]

    # Asegurar que la población sea positiva (descartar registros inválidos)
    df = df[df["population"] > 0]

    # Reemplazar valores nulos en columnas críticas con 0
    # Esto aplica para "new_cases" y "people_vaccinated" si existen
    for col in ["new_cases", "people_vaccinated"]:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Eliminar filas duplicadas según la combinación de location + date
    df = df.drop_duplicates(subset=["location", "date"])

    # Devolver el DataFrame ya limpio y consistente
    return df



# -----------------------------
# Asset 3: Chequeos de Entrada
# -----------------------------
@asset(
    description="Ejecuta chequeos de integridad sobre el DataFrame limpio",
    check_specs=[
        # Se definen los diferentes chequeos que este asset validará
        AssetCheckSpec(name="fechas_validas", description="No hay fechas futuras", asset="chequeos_entrada"),
        AssetCheckSpec(name="columnas_clave_no_nulas", description="location, date, population no nulas", asset="chequeos_entrada"),
        AssetCheckSpec(name="unicidad_location_date", description="No hay duplicados en location-date", asset="chequeos_entrada"),
        AssetCheckSpec(name="population_positiva", description="population > 0", asset="chequeos_entrada"),
        AssetCheckSpec(name="new_cases_validos", description="No hay new_cases negativos", asset="chequeos_entrada"),
    ]
)
def chequeos_entrada(datos_limpios: pd.DataFrame):
    # Crear una copia del DataFrame limpio
    df = datos_limpios.copy()

    # Guardar la fecha de hoy para validaciones
    hoy = pd.to_datetime(datetime.today().date())

    # -------------------------------
    # Chequeo 1: Fechas válidas
    # Validar que no existan fechas mayores a la fecha actual
    # -------------------------------
    yield AssetCheckResult(
        check_name="fechas_validas",
        passed=bool(df["date"].max() <= hoy),
        metadata={"fecha_maxima": str(df["date"].max())}  # Guardar la última fecha encontrada
    )

    # -------------------------------
    # Chequeo 2: Columnas clave no nulas
    # Verificar que location, date y population no contengan valores vacíos
    # -------------------------------
    filas_afectadas = int(df[["location", "date", "population"]].isna().any(axis=1).sum())
    yield AssetCheckResult(
        check_name="columnas_clave_no_nulas",
        passed=filas_afectadas == 0,
        metadata={"filas_afectadas": filas_afectadas},  # Número de filas con valores faltantes
    )

    # -------------------------------
    # Chequeo 3: Unicidad location-date
    # Confirmar que no existan duplicados por país y fecha
    # -------------------------------
    duplicados = int(df.duplicated(subset=["location", "date"]).sum())
    yield AssetCheckResult(
        check_name="unicidad_location_date",
        passed=duplicados == 0,
        metadata={"duplicados": duplicados},  # Número de filas duplicadas detectadas
    )

    # -------------------------------
    # Chequeo 4: Population positiva
    # Validar que no existan poblaciones con valores <= 0
    # -------------------------------
    pop_invalid = int((df["population"] <= 0).sum())
    yield AssetCheckResult(
        check_name="population_positiva",
        passed=pop_invalid == 0,
        metadata={"pop_invalidas": pop_invalid},  # Número de filas con población inválida
    )

    # -------------------------------
    # Chequeo 5: new_cases válidos
    # Verificar que los casos nuevos no tengan valores negativos
    # -------------------------------
    negativos = int((df["new_cases"] < 0).sum())
    yield AssetCheckResult(
        check_name="new_cases_validos",
        passed=negativos == 0,
        metadata={"negativos_encontrados": negativos},  # Número de valores negativos encontrados
    )

    # -------------------------------
    # Output final del asset
    # Retornar el DataFrame como salida para que pueda ser usado por otros assets
    # -------------------------------
    yield Output(df)



# -----------------------------
# Asset 4: Procesamiento (Ecuador y Perú)
# -----------------------------
@asset(description="Filtra, limpia y selecciona columnas esenciales para Ecuador y Perú.")
def datos_procesados(datos_limpios: pd.DataFrame) -> pd.DataFrame:
    # Crear una copia del DataFrame limpio para no modificar el original
    df = datos_limpios.copy()

    # Filtrar únicamente los países definidos en la lista PAISES (Ecuador y Perú)
    df = df[df["location"].isin(PAISES)]

    # Seleccionar únicamente las columnas más relevantes para el análisis:
    # - location: país
    # - date: fecha
    # - new_cases: casos diarios
    # - people_vaccinated: número de personas vacunadas
    # - population: población total
    columnas = ["location", "date", "new_cases", "people_vaccinated", "population"]
    df = df[columnas]

    # Devolver el DataFrame ya filtrado y con solo las columnas necesarias
    return df


# -----------------------------
# Asset 5: Tabla de Perfilado
# -----------------------------
@asset(description="Genera tabla de perfilado básico del DataFrame procesado")
def tabla_perfilado(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    # Crear una copia del DataFrame procesado para no modificarlo directamente
    df = datos_procesados.copy()

    # Crear una tabla de perfilado con estadísticas básicas:
    # - columna: nombre de la variable analizada
    # - minimo: valor mínimo encontrado
    # - maximo: valor máximo encontrado
    # - porcentaje_faltante: % de valores nulos en esa columna
    perfilado = pd.DataFrame({
        "columna": ["new_cases", "people_vaccinated", "date"],
        
        # Valores mínimos
        "minimo": [
            float(df["new_cases"].min()),               # Mínimo de casos nuevos
            float(df["people_vaccinated"].min()),       # Mínimo de personas vacunadas
            pd.to_datetime(df["date"]).min(),           # Fecha mínima registrada
        ],
        
        # Valores máximos
        "maximo": [
            float(df["new_cases"].max()),               # Máximo de casos nuevos
            float(df["people_vaccinated"].max()),       # Máximo de personas vacunadas
            pd.to_datetime(df["date"]).max(),           # Fecha máxima registrada
        ],
        
        # Porcentaje de datos faltantes en cada columna
        "porcentaje_faltante": [
            float(df["new_cases"].isna().mean() * 100),        # % de nulos en casos nuevos
            float(df["people_vaccinated"].isna().mean() * 100),# % de nulos en vacunados
            float(df["date"].isna().mean() * 100),             # % de nulos en fechas
        ],
    })

    # Guardar la tabla de perfilado en un archivo CSV para consulta externa
    perfilado.to_csv("tabla_perfilado.csv", index=False)

    # Retornar el DataFrame de perfilado para ser usado en otros procesos
    return perfilado



# -----------------------------
# Asset 6: Métrica incidencia acumulada 7d
# -----------------------------
@asset(description="Incidencia acumulada a 7 días por 100 mil habitantes")
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    # Crear una copia del DataFrame para no modificar el original
    df = datos_procesados.copy()

    # -------------------------------
    # Cálculo de incidencia diaria
    # La incidencia diaria se calcula como casos nuevos por cada 100.000 habitantes
    # -------------------------------
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100000

    # -------------------------------
    # Promedio móvil de 7 días (incidencia acumulada)
    # Se agrupa por país y se calcula la media móvil sobre incidencia_diaria
    # window=7 -> ventana de 7 días
    # min_periods=1 -> permite calcular incluso si hay menos de 7 días disponibles
    # -------------------------------
    df["incidencia_7d"] = (
        df.groupby("location")["incidencia_diaria"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    # -------------------------------
    # Selección y renombrado de columnas
    # Solo se incluyen las columnas necesarias y se renombra a español
    # - location -> pais
    # - date -> fecha
    # -------------------------------
    result = df[["date", "location", "incidencia_7d"]].rename(
        columns={"location": "pais", "date": "fecha"}
    )

    # -------------------------------
    # Guardar resultados en CSV
    # Se guarda para análisis posterior o uso externo
    # -------------------------------
    result.to_csv("metrica_incidencia_7d.csv", index=False)

    # -------------------------------
    # Retornar el DataFrame con la incidencia acumulada
    # Este output puede ser usado por otros assets del pipeline
    # -------------------------------
    return result



# -----------------------------
# Asset 7: Métrica factor de crecimiento semanal
# -----------------------------
@asset(description="Factor de crecimiento semanal (7 días)")
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    # Crear una copia del DataFrame para no modificar el original
    df = datos_procesados.copy()

   
    # Ordenar los datos por país y fecha
    # Es importante para que los cálculos de rolling y shift sean correctos
   
    df = df.sort_values(["location", "date"])

    
    # Casos acumulados en ventana de 7 días
    # Se calcula la suma de casos nuevos por país en bloques de 7 días
    # min_periods=7 asegura que solo se calcula si hay 7 días completos
    
    df["casos_semana"] = (
        df.groupby("location")["new_cases"]
        .transform(lambda x: x.rolling(window=7, min_periods=7).sum())
    )

   
    # Casos de la semana previa
    # Se desplaza 7 días para comparar con la semana anterior
    
    df["casos_semana_prev"] = (
        df.groupby("location")["casos_semana"].shift(7)
    )

    
    # Factor de crecimiento semanal
    # Se calcula como la razón: casos de la semana actual / casos de la semana anterior
    
    df["factor_crec_7d"] = df["casos_semana"] / df["casos_semana_prev"]

    
    # Selección y renombrado de columnas
    # Se dejan solo las columnas necesarias y se renombran a español
    # - location -> pais
    # - date -> semana_fin
    
    result = df[["date", "location", "casos_semana", "factor_crec_7d"]].rename(
        columns={"location": "pais", "date": "semana_fin"}
    )

  
    # Guardar resultados en CSV
 
    result.to_csv("metrica_factor_crec_7d.csv", index=False)

    
    # Retornar el DataFrame con el factor de crecimiento semanal
   
    return result



# -----------------------------
# Paso 5: Chequeos de Salida
# -----------------------------
@asset(
    description="Chequeos de salida sobre métricas calculadas",
    check_specs=[
        # Definición del chequeo: la incidencia de 7 días debe estar entre 0 y 2000
        AssetCheckSpec(
            name="incidencia_7d_valida", 
            description="0 <= incidencia_7d <= 2000", 
            asset="chequeos_salida"
        ),
    ]
)
def chequeos_salida(metrica_incidencia_7d: pd.DataFrame):
    # Crear una copia del DataFrame de métricas para no modificar el original
    df = metrica_incidencia_7d.copy()

    # Contar cuántos valores de incidencia están fuera del rango permitido (0 a 2000)
    fuera_rango = int(((df["incidencia_7d"] < 0) | (df["incidencia_7d"] > 2000)).sum())

    # Emitir el resultado del chequeo de integridad
    # passed será True si no hay valores fuera de rango
    yield AssetCheckResult(
        check_name="incidencia_7d_valida",
        passed=fuera_rango == 0,
        metadata={"valores_fuera_rango": fuera_rango}  # Información adicional para debugging
    )

    # Retornar el DataFrame como output para otros assets
    yield Output(df)


# -----------------------------
# Paso 6: Exportación de Resultados
# -----------------------------
@asset(description="Exportar resultados finales (procesados y métricas) a Excel")
def reporte_excel_covid(
    datos_procesados: pd.DataFrame,
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    # Nombre del archivo Excel que contendrá los resultados
    filename = "reporte_covid.xlsx"

    # Crear un ExcelWriter usando xlsxwriter como motor
    # Esto permite guardar múltiples hojas en un solo archivo
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        # Guardar el DataFrame de datos procesados en la hoja "Datos Procesados"
        datos_procesados.to_excel(writer, sheet_name="Datos Procesados", index=False)

        # Guardar la métrica de incidencia a 7 días en la hoja "Incidencia_7d"
        metrica_incidencia_7d.to_excel(writer, sheet_name="Incidencia_7d", index=False)

        # Guardar la métrica del factor de crecimiento semanal en la hoja "Factor_Crec_7d"
        metrica_factor_crec_7d.to_excel(writer, sheet_name="Factor_Crec_7d", index=False)

    # Retornar el nombre del archivo generado
    return filename
