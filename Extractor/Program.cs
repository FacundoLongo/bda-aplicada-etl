using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Extractor
{
    internal class Program
    {
        /// <summary>
        /// Este idOrigen tiene que existir en la BD en la tabla Dim_Origen, revisar antes de ejecutar!
        /// </summary>
        private static int idOrigen = 1;

        private static Dim_Origen Dim_Origen = null;

        private static List<Dim_Pais> Dim_Paises = null;
        private static List<Dim_Anio> Dim_Anios = null;
        private static List<Dim_Hora> Dim_Horas = null;

        private static string DatosExtraidos = null;

        private static List<Fact_Temperatura> Fact_Temperaturas = null;

        private static DateTime FechaInicio = new DateTime(2000, 1, 1);
        private static DateTime FechaFin = DateTime.Now;

        //private static string ConexionBD = "Data Source=localhost\\SQLEXPRESS; Initial Catalog=bdaplicada; Integrated Security=true";
        private static string ConexionBD = "Server=tcp:devsqlservermulti.database.windows.net,1433;Initial Catalog=bdaplicada;User ID=javier@devsqlservermulti;Password=aplicada123!;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";

        static async Task Main(string[] args)
        {
            //----------------------------------------
            //CARGAMOS DIMENSIONES EN MEMORIA PARA AGILIZAR EL TEMA
            //----------------------------------------
            CargarDimensionesDinamicamente();
            CargarDimensiones();

            if (Dim_Origen == null)
                throw new Exception("Faltó cargar el origen de datos en la BD!");

            if (Dim_Paises == null)
                throw new Exception("Faltó cargar los países en la BD!");

            if (Dim_Anios == null)
                throw new Exception("Faltó cargar los años en la BD!");

            if (Dim_Horas == null)
                throw new Exception("Faltó cargar las horas en la BD!");

            //----------------------------------------
            //ESTO ES EL ETL EN SÍ MISMO
            //----------------------------------------

            foreach (Dim_Pais dim_Pais in Dim_Paises)
            {
                Console.WriteLine($"Procesando país: {dim_Pais.Pais}");

                foreach (Dim_Localidad dim_Localidad in dim_Pais.Localidades)
                {
                    Console.WriteLine($"Procesando localidad: Latitud {dim_Localidad.Latitud}, Longitud {dim_Localidad.Longitud}");

                    DatosExtraidos = null;
                    Fact_Temperaturas = new List<Fact_Temperatura>();

                    // Llamada asíncrona a la extracción de datos
                    await ExtraerDatosAsync(dim_Localidad);

                    Console.WriteLine($"Datos extraídos para la localidad.");

                    TransformarDatos(dim_Localidad);

                    Console.WriteLine($"Datos transformados para la localidad.");

                    CargarDatos();

                    Console.WriteLine($"Datos cargados en la base de datos para la localidad.");
                }
            }
        }


        private static void CargarDimensiones()
        {
            //CargarDimensionesDinamicamente();

            SqlConnection conexion = null;

            try
            {
                conexion = new SqlConnection();
                conexion.ConnectionString = ConexionBD;

                conexion.Open();

                #region Cargamos Origen

                string sql = "SELECT * FROM Dim_Origen WHERE ID_Origen = @ID_Origen";

                SqlCommand comando = conexion.CreateCommand();
                comando.CommandType = CommandType.Text;
                comando.CommandText = sql;

                SqlParameter parameter = new SqlParameter("@ID_Origen", idOrigen);
                comando.Parameters.Add(parameter);

                SqlDataReader dataReader = comando.ExecuteReader();

                if (dataReader.HasRows)
                {
                    dataReader.Read();

                    Dim_Origen = new Dim_Origen();

                    Dim_Origen.ID_Origen = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Origen")).Value;
                    Dim_Origen.Origen = dataReader.GetSqlString(dataReader.GetOrdinal("Origen")).Value;
                    Dim_Origen.Url = dataReader.GetSqlString(dataReader.GetOrdinal("URL")).Value;
                    Dim_Origen.Confiabilidad = (float)dataReader.GetSqlDecimal(dataReader.GetOrdinal("Confiabilidad")).Value;
                }

                dataReader.Close();

                #endregion

                #region Obtener países ya procesados
                HashSet<int> paisesProcesados = new HashSet<int>();
                string sqlProcesados = @"
            SELECT DISTINCT l.ID_Pais
            FROM Fact_Temperatura ft
            JOIN Dim_Localidad l ON ft.ID_Localidad = l.ID_Localidad";

                SqlCommand comandoProcesados = new SqlCommand(sqlProcesados, conexion);
                SqlDataReader readerProcesados = comandoProcesados.ExecuteReader();

                while (readerProcesados.Read())
                {
                    paisesProcesados.Add(readerProcesados.GetInt32(0));
                }
                readerProcesados.Close();
                #endregion

                #region Cargar Dim_Pais excluyendo procesados
                Dim_Paises = new List<Dim_Pais>();
                string sqlPaises = "SELECT * FROM Dim_Pais";
                SqlCommand comandoPaises = new SqlCommand(sqlPaises, conexion);
                SqlDataReader readerPaises = comandoPaises.ExecuteReader();

                while (readerPaises.Read())
                {
                    int idPais = readerPaises.GetInt32(readerPaises.GetOrdinal("ID_Pais"));

                    if (!paisesProcesados.Contains(idPais)) // Excluir países ya procesados
                    {
                        Dim_Pais dimPais = new Dim_Pais
                        {
                            ID_Pais = idPais,
                            Pais = readerPaises.GetString(readerPaises.GetOrdinal("Pais")),
                            Hemisferio = readerPaises.GetString(readerPaises.GetOrdinal("Hemisferio"))
                        };

                        Dim_Paises.Add(dimPais);
                    }
                }
                readerPaises.Close();
                #endregion

                #region Cargamos Localidades

                foreach (Dim_Pais dim_Pais in Dim_Paises)
                {
                    sql = "SELECT * FROM Dim_Localidad WHERE ID_Pais = @ID_Pais";

                    comando = conexion.CreateCommand();
                    comando.CommandType = CommandType.Text;
                    comando.CommandText = sql;

                    parameter = new SqlParameter("@ID_Pais", dim_Pais.ID_Pais);
                    comando.Parameters.Add(parameter);

                    dataReader = comando.ExecuteReader();

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            Dim_Localidad dim_Localidad = new Dim_Localidad();

                            dim_Localidad.ID_Localidad = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Localidad")).Value;
                            dim_Localidad.ID_Pais = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Pais")).Value;
                            dim_Localidad.Latitud = (float)dataReader.GetSqlDecimal(dataReader.GetOrdinal("Latitud")).Value;
                            dim_Localidad.Longitud = (float)dataReader.GetSqlDecimal(dataReader.GetOrdinal("Longitud")).Value;
                            dim_Pais.Localidades.Add(dim_Localidad);
                        }
                    }

                    dataReader.Close();
                }

                #endregion

                #region Cargamos Anios

                sql = "SELECT * FROM Dim_Anio";

                comando = conexion.CreateCommand();
                comando.CommandType = CommandType.Text;
                comando.CommandText = sql;

                dataReader = comando.ExecuteReader();

                if (dataReader.HasRows)
                {
                    Dim_Anios = new List<Dim_Anio>();

                    while (dataReader.Read())
                    {
                        Dim_Anio dim_Anio = new Dim_Anio();

                        dim_Anio.ID_Anio = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Anio")).Value;
                        dim_Anio.Anio = dataReader.GetSqlInt32(dataReader.GetOrdinal("Anio")).Value;
                        Dim_Anios.Add(dim_Anio);
                    }
                }

                dataReader.Close();

                #endregion

                #region Cargamos Meses

                foreach (Dim_Anio dim_Anio in Dim_Anios)
                {
                    sql = "SELECT * FROM Dim_Mes WHERE ID_Anio = @ID_Anio";

                    comando = conexion.CreateCommand();
                    comando.CommandType = CommandType.Text;
                    comando.CommandText = sql;

                    parameter = new SqlParameter("@ID_Anio", dim_Anio.ID_Anio);
                    comando.Parameters.Add(parameter);

                    dataReader = comando.ExecuteReader();

                    if (dataReader.HasRows)
                    {
                        while (dataReader.Read())
                        {
                            Dim_Mes dim_Mes = new Dim_Mes();

                            dim_Mes.ID_Mes = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Mes")).Value;
                            dim_Mes.ID_Anio = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Anio")).Value;
                            dim_Mes.Mes = dataReader.GetSqlInt32(dataReader.GetOrdinal("Mes")).Value;
                            dim_Mes.Nombre = dataReader.GetSqlString(dataReader.GetOrdinal("Nombre")).Value;
                            dim_Anio.Meses.Add(dim_Mes);
                        }
                    }

                    dataReader.Close();
                }

                #endregion

                #region Cargamos Dias

                foreach (Dim_Anio dim_Anio in Dim_Anios)
                {
                    foreach (Dim_Mes dim_Mes in dim_Anio.Meses)
                    {
                        sql = "SELECT * FROM Dim_Dia WHERE ID_Mes = @ID_Mes";

                        comando = conexion.CreateCommand();
                        comando.CommandType = CommandType.Text;
                        comando.CommandText = sql;

                        parameter = new SqlParameter("@ID_Mes", dim_Mes.ID_Mes);
                        comando.Parameters.Add(parameter);

                        dataReader = comando.ExecuteReader();

                        if (dataReader.HasRows)
                        {
                            while (dataReader.Read())
                            {
                                Dim_Dia dim_Dia = new Dim_Dia();

                                dim_Dia.ID_Dia = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Dia")).Value;
                                dim_Dia.ID_Mes = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Mes")).Value;
                                dim_Dia.Dia = dataReader.GetSqlInt32(dataReader.GetOrdinal("Dia")).Value;
                                dim_Dia.Nombre = dataReader.GetSqlString(dataReader.GetOrdinal("Nombre")).Value;
                                dim_Mes.Dias.Add(dim_Dia);
                            }
                        }

                        dataReader.Close();
                    }
                }

                #endregion

                #region Cargamos Horas

                sql = "SELECT * FROM Dim_Hora";

                comando = conexion.CreateCommand();
                comando.CommandType = CommandType.Text;
                comando.CommandText = sql;

                dataReader = comando.ExecuteReader();

                if (dataReader.HasRows)
                {
                    Dim_Horas = new List<Dim_Hora>();

                    while (dataReader.Read())
                    {
                        Dim_Hora dim_Hora = new Dim_Hora();

                        dim_Hora.ID_Hora = dataReader.GetSqlInt32(dataReader.GetOrdinal("ID_Hora")).Value;
                        dim_Hora.Hora = (TimeSpan)dataReader.GetSqlValue(dataReader.GetOrdinal("Hora"));
                        Dim_Horas.Add(dim_Hora);
                    }
                }

                dataReader.Close();

                #endregion

                conexion.Close();
            }
            catch (Exception ex)
            {
                if (conexion != null)
                    conexion.Close();
            }
            finally
            {
                if (conexion != null)
                    conexion.Dispose();
            }
        }

        private static void ExtraerDatos(Dim_Localidad dim_Localidad)
        {
            HttpClient httpClient = new HttpClient();
            Task<HttpResponseMessage> httpResponse = null;
            Task<string> contenido = null;

            string urlServicio = Dim_Origen.Url;
            urlServicio = string.Format(urlServicio, dim_Localidad.Latitud, dim_Localidad.Longitud, FechaInicio.ToString("yyyy-MM-dd"), FechaFin.ToString("yyyy-MM-dd"));

            Console.WriteLine($"Llamando a la API para la localidad: Latitud {dim_Localidad.Latitud}, Longitud {dim_Localidad.Longitud}");

            httpResponse = httpClient.GetAsync(urlServicio);
            httpResponse.Wait();

            if (httpResponse.IsCompleted && httpResponse.Result.StatusCode == System.Net.HttpStatusCode.OK)
            {
                contenido = httpResponse.Result.Content.ReadAsStringAsync();
                contenido.Wait();

                if (contenido.IsCompleted)
                {
                    DatosExtraidos = contenido.Result;
                }
            }

            Console.WriteLine($"Esperando 1 segundo antes de la próxima llamada...");
            System.Threading.Thread.Sleep(1000); // 1 segundo
        }

        private static async Task ExtraerDatosAsync(Dim_Localidad dim_Localidad)
        {
            HttpClient httpClient = null;

            // Construcción del URL
            string urlServicio = Dim_Origen.Url;
            urlServicio = string.Format(
                urlServicio,
                dim_Localidad.Latitud.ToString("F6", System.Globalization.CultureInfo.InvariantCulture),
                dim_Localidad.Longitud.ToString("F6", System.Globalization.CultureInfo.InvariantCulture),
                FechaInicio.ToString("yyyy-MM-dd"),
                FechaFin.ToString("yyyy-MM-dd")
            );

            // Log para depurar el URL
            Console.WriteLine($"Llamando a la API con URL: {urlServicio}\n");

            try
            {
                httpClient = new HttpClient();
                // Llamada a la API
                HttpResponseMessage httpResponse = await httpClient.GetAsync(urlServicio);

                // Manejo de la respuesta
                if (httpResponse.IsSuccessStatusCode)
                {
                    DatosExtraidos = await httpResponse.Content.ReadAsStringAsync();
                    Console.WriteLine($"Datos extraídos correctamente para la localidad (Lat: {dim_Localidad.Latitud}, Lon: {dim_Localidad.Longitud}).\n");
                }
                else
                {
                    // Leer el contenido de la respuesta en caso de error
                    string errorContent = await httpResponse.Content.ReadAsStringAsync();
                    Console.WriteLine($"Error al llamar a la API (Código HTTP: {httpResponse.StatusCode}). Detalles: {errorContent}\n");
                }
            }
            catch (HttpRequestException ex)
            {
                // Manejo de errores relacionados con la solicitud HTTP
                Console.WriteLine($"Error en la solicitud HTTP: {ex.Message}\n");
            }
            catch (TaskCanceledException ex)
            {
                // Manejo de errores por tiempo de espera agotado
                Console.WriteLine($"Tiempo de espera agotado para la solicitud HTTP: {ex.Message}\n");
            }
            catch (Exception ex)
            {
                // Manejo de otros errores
                Console.WriteLine($"Error inesperado al llamar a la API: {ex.Message}\n");
            }
            finally
            {
                // Pausa antes de la siguiente llamada
                Console.WriteLine("Esperando 1 segundo antes de la próxima llamada...\n");
                await Task.Delay(1000); // 1 segundo
            }
        }




        private static void TransformarDatos(Dim_Localidad dim_Localidad)
        {
            if (DatosExtraidos != null)
            {
                var datos = JsonConvert.DeserializeObject<dynamic>(DatosExtraidos);

                //CONVERTIR EN CASO QUE SEA NECESARIO

                int offsetSeconds = datos.utc_offset_seconds;
                string timeZone = datos.timezone;

                var hourly = datos.hourly;

                for (int indice=0; indice<hourly.time.Count; indice++)
                {
                    if (hourly.time[indice] == null || hourly.temperature_2m[indice] == null)
                        continue;

                    //SI ES UN STRING, CONVERTIR A FECHA...

                    DateTime fecha = hourly.time[indice];
                    fecha = fecha.AddHours(-3); //ESTAMOS EN ARGENTINA!

                    //SI ES UN STRING, CONVERTIR A FLOAT

                    float temperatura = hourly.temperature_2m[indice];
                    int humedad = hourly.relative_humidity_2m[indice];
                    float sensaciontermica = hourly.apparent_temperature[indice];
                    float precipitacion = hourly.precipitation[indice];


                    //----------------------------------
                    //BUSCAMOS ID'S EN DIMENSIONES PARA COMPLETAR LOS DATOS
                    //----------------------------------

                    Dim_Anio dim_Anio = Dim_Anios.FirstOrDefault(p => p.Anio == fecha.Year);

                    if (dim_Anio != null)
                    {
                        Dim_Mes dim_Mes = dim_Anio.Meses.FirstOrDefault(p => p.Mes == fecha.Month);

                        if (dim_Mes != null)
                        {
                            Dim_Dia dim_Dia = dim_Mes.Dias.FirstOrDefault(p => p.Dia == fecha.Day);

                            if (dim_Dia != null)
                            {
                                Dim_Hora dim_Hora = Dim_Horas.FirstOrDefault(p => p.Hora.Hours == fecha.Hour);

                                if (dim_Hora != null)
                                {
                                    Fact_Temperatura fact_Temperatura = new Fact_Temperatura();
                                    fact_Temperatura.ID_Origen = Dim_Origen.ID_Origen;
                                    fact_Temperatura.ID_Dia = dim_Dia.ID_Dia;
                                    fact_Temperatura.ID_Hora = dim_Hora.ID_Hora;
                                    fact_Temperatura.ID_Localidad = dim_Localidad.ID_Localidad;
                                    fact_Temperatura.Temperatura = temperatura;
                                    fact_Temperatura.Humedad = humedad;
                                    fact_Temperatura.SensacionTermica = sensaciontermica;
                                    fact_Temperatura.Precipitacion = precipitacion;
                                    Fact_Temperaturas.Add(fact_Temperatura);
                                }
                            }
                        }
                    }
                }
            }
        }

        private static void CargarDimensionesDinamicamente()
        {
            SqlConnection conexion = null;

            try
            {
                conexion = new SqlConnection(ConexionBD);
                conexion.Open();

                // Obtener el rango de años a procesar
                int anioActual = DateTime.Now.Year;
                int anioInicio = anioActual - 9; // Últimos 10 años

                // Verificar si las dimensiones de tiempo existen
                string checkSql = "SELECT COUNT(*) FROM Dim_Anio WHERE Anio BETWEEN @AnioInicio AND @AnioFin";
                SqlCommand checkCmd = new SqlCommand(checkSql, conexion);
                checkCmd.Parameters.AddWithValue("@AnioInicio", anioInicio);
                checkCmd.Parameters.AddWithValue("@AnioFin", anioActual);

                int count = (int)checkCmd.ExecuteScalar();

                if (count == (anioActual - anioInicio + 1))
                {
                    Console.WriteLine("Las dimensiones de tiempo ya están completas.");
                    return; // Las dimensiones ya están cargadas
                }

                Console.WriteLine("Generando dimensiones de tiempo...");

                // Insertar Años, Meses y Días dinámicamente
                int idAnio = 1, idMes = 1, idDia = 1;

                for (int anio = anioInicio; anio <= anioActual; anio++)
                {
                    // Insertar Año si no existe
                    string insertAnioSql = @"
                    IF NOT EXISTS (SELECT 1 FROM Dim_Anio WHERE Anio = @Anio)
                    BEGIN
                        INSERT INTO Dim_Anio (ID_Anio, Anio) VALUES (@ID_Anio, @Anio)
                    END";
                    SqlCommand insertAnioCmd = new SqlCommand(insertAnioSql, conexion);
                    insertAnioCmd.Parameters.AddWithValue("@ID_Anio", idAnio);
                    insertAnioCmd.Parameters.AddWithValue("@Anio", anio);
                    insertAnioCmd.ExecuteNonQuery();

                    for (int mes = 1; mes <= 12; mes++)
                    {
                        // Insertar Mes si no existe
                        string nombreMes = new DateTime(anio, mes, 1).ToString("MMMM");
                        string insertMesSql = @"
                        IF NOT EXISTS (SELECT 1 FROM Dim_Mes WHERE ID_Anio = @ID_Anio AND Mes = @Mes)
                        BEGIN
                            INSERT INTO Dim_Mes (ID_Mes, ID_Anio, Mes, Nombre) VALUES (@ID_Mes, @ID_Anio, @Mes, @Nombre)
                        END";
                        SqlCommand insertMesCmd = new SqlCommand(insertMesSql, conexion);
                        insertMesCmd.Parameters.AddWithValue("@ID_Mes", idMes);
                        insertMesCmd.Parameters.AddWithValue("@ID_Anio", idAnio);
                        insertMesCmd.Parameters.AddWithValue("@Mes", mes);
                        insertMesCmd.Parameters.AddWithValue("@Nombre", nombreMes);
                        insertMesCmd.ExecuteNonQuery();

                        int diasEnMes = DateTime.DaysInMonth(anio, mes);

                        for (int dia = 1; dia <= diasEnMes; dia++)
                        {
                            // Insertar Día si no existe
                            string nombreDia = new DateTime(anio, mes, dia).ToString("dddd");
                            string insertDiaSql = @"
                            IF NOT EXISTS (SELECT 1 FROM Dim_Dia WHERE ID_Mes = @ID_Mes AND Dia = @Dia)
                            BEGIN
                                INSERT INTO Dim_Dia (ID_Dia, ID_Mes, Dia, Nombre) VALUES (@ID_Dia, @ID_Mes, @Dia, @Nombre)
                            END";
                            SqlCommand insertDiaCmd = new SqlCommand(insertDiaSql, conexion);
                            insertDiaCmd.Parameters.AddWithValue("@ID_Dia", idDia);
                            insertDiaCmd.Parameters.AddWithValue("@ID_Mes", idMes);
                            insertDiaCmd.Parameters.AddWithValue("@Dia", dia);
                            insertDiaCmd.Parameters.AddWithValue("@Nombre", nombreDia);
                            insertDiaCmd.ExecuteNonQuery();

                            idDia++;
                        }

                        idMes++;
                    }

                    idAnio++;
                }

                Console.WriteLine("Dimensiones de tiempo generadas exitosamente.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error al crear las dimensiones: " + ex.Message);
            }
            finally
            {
                if (conexion != null)
                    conexion.Dispose();
            }
        }
 
        private static void CargarDatos()
        {
            if (Fact_Temperaturas != null)
            {
                SqlConnection conexion = null;

                try
                {
                    conexion = new SqlConnection();
                    conexion.ConnectionString = ConexionBD;

                    conexion.Open();

                    foreach (Fact_Temperatura fact_Temperatura in Fact_Temperaturas)
                    {
                        string sql = "INSERT INTO Fact_Temperatura (ID_Origen, ID_Dia, ID_Hora, ID_Localidad, Temperatura, Humedad, SensacionTermica, Precipitacion) VALUES (@ID_Origen, @ID_Dia, @ID_Hora, @ID_Localidad, @Temperatura, @Humedad, @SensacionTermica, @Precipitacion)";

                        SqlCommand comando = conexion.CreateCommand();
                        comando.CommandType = CommandType.Text;
                        comando.CommandText = sql;

                        SqlParameter parameter = new SqlParameter("@ID_Origen", fact_Temperatura.ID_Origen);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@ID_Dia", fact_Temperatura.ID_Dia);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@ID_Hora", fact_Temperatura.ID_Hora);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@ID_Localidad", fact_Temperatura.ID_Localidad);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@Temperatura", fact_Temperatura.Temperatura);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@Humedad", fact_Temperatura.Humedad);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@SensacionTermica", fact_Temperatura.SensacionTermica);
                        comando.Parameters.Add(parameter);

                        parameter = new SqlParameter("@Precipitacion", fact_Temperatura.Precipitacion);
                        comando.Parameters.Add(parameter);

                        comando.ExecuteNonQuery();
                    }

                    conexion.Close();
                }
                catch (Exception ex)
                {
                    if (conexion != null)
                        conexion.Close();
                }
                finally
                {
                    if (conexion != null)
                        conexion.Dispose();
                }
            }
        }
    }
}