# proyecto_final
## README del DAG

### Descripción
Este DAG de Airflow (`api_project_dag`) orquesta un proceso ETL (Extractar, Transformar, Cargar) que involucra datos de una API y archivos CSV relacionados con información de automóviles. El DAG está diseñado para ejecutarse en un horario diario.

### Estructura del DAG
- **ID del DAG**: `api_project_dag`
- **Argumentos por Defecto**:
  - `owner`: airflow
  - `depends_on_past`: False
  - `start_date`: 13 de septiembre de 2023
  - `email`: ['airflow@example.com']
  - `email_on_failure`: False
  - `email_on_retry`: False
  - `retries`: 1
  - `retry_delay`: 1 minuto

### Tareas
1. **read_api**:
   - ID de la Tarea: `read_api`
   - Operador: PythonOperator
   - Función Llamable: `read_df_unido`
   - Contexto Proporcionado: True
   - Descripción: Lee datos de una API.

2. **transform_api**:
   - ID de la Tarea: `transform_api`
   - Operador: PythonOperator
   - Función Llamable: `transform_API`
   - Contexto Proporcionado: True
   - Descripción: Transforma los datos obtenidos de la API.

3. **read_carros**:
   - ID de la Tarea: `read_carros`
   - Operador: PythonOperator
   - Función Llamable: `read_csv`
   - Contexto Proporcionado: True
   - Descripción: Lee información de automóviles desde archivos CSV.

4. **transform_carros**:
   - ID de la Tarea: `transform_carros`
   - Operador: PythonOperator
   - Función Llamable: `transform_datos`
   - Contexto Proporcionado: True
   - Descripción: Transforma datos de automóviles.

5. **load**:
   - ID de la Tarea: `load`
   - Operador: PythonOperator
   - Función Llamable: `load`
   - Contexto Proporcionado: True
   - Descripción: Carga los datos transformados.

6. **load_api**:
   - ID de la Tarea: `load_api`
   - Operador: PythonOperator
   - Función Llamable: `func1`
   - Contexto Proporcionado: True
   - Descripción: Llama a `func1` para imprimir la fecha actual.

### Dependencias
- La tarea `read_api` es seguida por `transform_api` y luego por `load_api`.
- La tarea `read_carros` es seguida por `transform_carros` y luego por `store`.

### Programación
El DAG está programado para ejecutarse diariamente.

