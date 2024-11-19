
# ETL Climático en C#

## Descripción del Proyecto

Este proyecto implementa una **ETL (Extract, Transform, Load)** en **C#** que extrae datos climáticos de una API externa, los transforma en un formato estructurado y los carga en una base de datos SQL siguiendo un modelo dimensional de copo de nieve. El objetivo principal es procesar grandes volúmenes de datos climáticos (temperatura, humedad, sensación térmica y precipitación) para su análisis y visualización, brindando información valiosa sobre el cambio climático.
Fue desarrollado para la presentación de Base de Datos Aplicada.

## Características Principales

- **Extracción de Datos**: Consume una API de datos climáticos utilizando `HttpClient` y realiza solicitudes para obtener datos históricos y actuales basados en coordenadas geográficas.
- **Transformación**: Ajusta los datos al modelo dimensional realizando operaciones como normalización de fechas, validación de métricas y asignación de claves para las dimensiones.
- **Carga en la Base de Datos**: Almacena los datos procesados en una tabla de hechos relacional (`Fact_Temperatura`) y mantiene la integridad referencial con las dimensiones relacionadas.

## Modelo Dimensional

El diseño del sistema sigue un esquema de **copo de nieve**, con las siguientes tablas principales:

### Dimensiones
- **Dim_Origen**: Información sobre la fuente de los datos (URL de la API, confiabilidad).
- **Dim_Pais** y **Dim_Localidad**: Organización geográfica de los datos.
- **Dim_Anio**, **Dim_Mes**, **Dim_Dia**, **Dim_Hora**: Dimensiones temporales que facilitan análisis a diferentes niveles de granularidad.

### Tabla de Hechos
- **Fact_Temperatura**: Contiene las métricas procesadas (temperatura, humedad, sensación térmica y precipitación), relacionadas con las dimensiones mencionadas.

## Requisitos del Sistema

- **Lenguaje**: C# (con .NET Framework o .NET 6.0).
- **Base de Datos**: SQL Server (compatible con cualquier sistema relacional que permita integración con ADO.NET).
- **Dependencias**:
  - `Newtonsoft.Json`: Para deserializar los datos JSON extraídos de la API.
  - **Conexión a la base de datos**: Configurada en el archivo de configuración del proyecto.

## Cómo Funciona

### 1. Extracción
- La ETL realiza solicitudes a la API externa utilizando coordenadas geográficas y rangos de fechas.
- Los datos se reciben en formato JSON y se almacenan temporalmente en memoria para su procesamiento.

### 2. Transformación
- Se convierten los datos al modelo dimensional. Por ejemplo:
  - Fechas y horas son normalizadas al huso horario local.
  - Las métricas son validadas para asegurar su integridad.
- Se asignan claves a partir de las dimensiones cargadas en memoria.

### 3. Carga
- Los datos procesados se insertan en la tabla de hechos `Fact_Temperatura` mediante sentencias SQL parametrizadas.
- Se verifican duplicados para evitar registros redundantes.

## Configuración del Proyecto

1. **Clonar el Repositorio**:
   ```bash
   git clone https://github.com/FacundoLongo/bda-aplicada-etl.git
   ```

2. **Configurar la Cadena de Conexión**:
   Editar la variable `ConexionBD` en el archivo `Program.cs`:
   ```csharp
   private static string ConexionBD = "Server=tcp:mi-servidor.database.windows.net,1433;Initial Catalog=mi-base;User ID=usuario;Password=contraseña;";
   ```

3. **Instalar Dependencias**:
   Asegurarse de que el proyecto tiene acceso a `Newtonsoft.Json` (se puede instalar desde NuGet).

4. **Configurar el ID del Origen**:
   En el archivo `Program.cs`, verificar que el `idOrigen` corresponda a un registro válido en la tabla `Dim_Origen` de la base de datos.

## Ejecución del Proyecto

1. **Compilar el Proyecto**:
   Utilizar Visual Studio o el CLI de .NET:
   ```bash
   dotnet build
   ```

2. **Ejecutar el Proyecto**:
   ```bash
   dotnet run
   ```

3. **Verificar los Resultados**:
   - Confirmar que los datos han sido insertados correctamente en la base de datos.
   - Los logs en la consola mostrarán el progreso del ETL y cualquier error encontrado.

## Estructura del Código

- **Program.cs**: Contiene el flujo principal del ETL.
- **Clases de Dimensiones**:
  - `Dim_Origen`, `Dim_Pais`, `Dim_Localidad`, etc.: Modelan las dimensiones de la base de datos.
- **Clase de Hechos**:
  - `Fact_Temperatura`: Representa los datos procesados listos para ser cargados.
- **Métodos Principales**:
  - `ExtraerDatosAsync`: Realiza la extracción de datos desde la API.
  - `TransformarDatos`: Ajusta los datos extraídos al modelo dimensional.
  - `CargarDatos`: Inserta los datos procesados en la base de datos.

## Visualización con Power BI

Los datos procesados por la ETL pueden ser conectados a **Power BI** para crear tableros interactivos. Esto permite visualizar métricas clave, analizar tendencias y comparar localidades geográficas. Se recomienda utilizar una conexión directa a la base de datos SQL para aprovechar actualizaciones automáticas en las visualizaciones.

## Problemas Comunes

1. **Error de Conexión a la API**:
   - Verificar la disponibilidad de la API y las coordenadas geográficas utilizadas.
   - Confirmar que la URL configurada en `Dim_Origen` sea válida.

2. **Errores de Base de Datos**:
   - Asegurarse de que la estructura de la base de datos coincida con el modelo dimensional.
   - Revisar los índices y restricciones de claves foráneas.

3. **Datos Duplicados**:
   - Confirmar que la lógica para evitar duplicados en la tabla `Fact_Temperatura` esté funcionando correctamente.
