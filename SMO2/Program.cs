using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Common;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;

namespace SMO2
{
    class Program
    {
        // Main
        static void Main(string[] args)
        {
            // Path a donde guardar el archivo
            string pathObjetos = ConfigurationManager.AppSettings["pathObjetos"].ToString();
            string pathSemillas = ConfigurationManager.AppSettings["pathSemillas"].ToString();

            // Instanciando conexión
            ServerConnection srvConn = new ServerConnection
            {
                ServerInstance = ConfigurationManager.AppSettings["serverInstance"].ToString(),
                LoginSecure = Boolean.Parse(ConfigurationManager.AppSettings["LoginSecure"].ToString()), // false para login con credenciales
                Login = ConfigurationManager.AppSettings["Login"].ToString(),
                Password = ConfigurationManager.AppSettings["Password"].ToString()

            };

            // Scripteando objetos y semillas
            if (scriptObjetosBD(pathObjetos, srvConn)) 
            {
                Console.WriteLine("::: FIN OBJETOS :::");

                if (scriptSemillasBD(pathSemillas, srvConn)) 
                {
                    Console.WriteLine("::: FIN SEMILLAS :::");
                }
            }

            Console.WriteLine("Presiona ENTER para salir...");
            Console.ReadLine();
        }

        // Función para crear objetos de la base de datos
        static Boolean scriptObjetosBD(string path, ServerConnection con) {
            // Instanciando
            Server srv = new Server(con);

            // Reference the database.  
            Database db = srv.Databases[ConfigurationManager.AppSettings["DB"].ToString()];

            // Instanciando el scripteador
            Scripter scrp = new Scripter(srv);
            var urns = new List<Urn>();

            // Propiedades del script
            scrp.Options.Triggers = true;               
            scrp.Options.FullTextIndexes = true;         
            scrp.Options.NoCollation = false;           
            scrp.Options.AnsiPadding = true;
            scrp.Options.XmlIndexes = true;
            scrp.Options.DriAll = true;
            scrp.Options.NonClusteredIndexes = true;
  
            // Obteniendo los esquemas de la BD
            foreach (Schema sch in db.Schemas)
            {
                // Si no es un objeto de SQL Server lo scripteamos
                if (!sch.IsSystemObject)
                {
                    urns.Add(sch.Urn);
                }
            }

            // Obteniendo las tablas de la BD   
            foreach (Table tb in db.Tables)
            {
                // check if the table is not a system table
                if (!tb.IsSystemObject)
                {
                    urns.Add(tb.Urn);
                }
            }

            // Obteniendo las vistas de la BD
            foreach (View view in db.Views)
            {
                // check if the view is not a system object
                if (!view.IsSystemObject)
                {
                    urns.Add(view.Urn);
                }
            }

            // Obteniendo los stored procedures de la BD   
            foreach (StoredProcedure sp in db.StoredProcedures)
            {
                // check if the procedure is not a system object
                if (!sp.IsSystemObject)
                {
                    urns.Add(sp.Urn);
                }
            }

            // Obteniendo los stored procedures de la BD   
            foreach (UserDefinedFunction udf in db.UserDefinedFunctions)
            {
                // check if the procedure is not a system object
                if (!udf.IsSystemObject)
                {
                    urns.Add(udf.Urn);
                }
            }

            // Instanciando un string builder para construir el script
            StringBuilder builder = new StringBuilder();
            System.Collections.Specialized.StringCollection sc = scrp.Script(urns.ToArray());
            foreach (string st in sc)
            {
                // Agregando los comandos al string builder
                builder.AppendLine(st);
                builder.AppendLine("GO"); // Se coloca GO al final de cada statement
            }

            // Escribiendo el archivo
            File.WriteAllText(path, builder.ToString());

            // Todo bien
            return true;
        }

        // Script semillas BD
        static Boolean scriptSemillasBD(string path, ServerConnection con) {

            // DataTable para tablas semillas
            DataTable dt = new DataTable();
            dt = getTablasSemilla();

            // Instanciando
            Server srv = new Server(con);

            // Reference the database.  
            Database db = srv.Databases[ConfigurationManager.AppSettings["DB"].ToString()];

            // Instanciando el string builder
            StringBuilder builder = new StringBuilder();

            // Contador
            int contador = 0;
            
            Scripter scrp = new Scripter(srv);
            ScriptingOptions options = new ScriptingOptions();
            options.DriAll = true;
            options.ClusteredIndexes = true;
            options.Default = true;
            options.Indexes = true;
            scrp.Options = options;

            Table[] tbls = new Table[db.Tables.Count];
            db.Tables.CopyTo(tbls, 0);
            scrp.Script(tbls);

            DependencyTree tree = scrp.DiscoverDependencies(tbls, true);
            DependencyWalker depwalker = new Microsoft.SqlServer.Management.Smo.DependencyWalker();
            DependencyCollection depcoll = depwalker.WalkDependencies(tree);

            foreach (DependencyCollectionNode dep in depcoll)
            {
                foreach (Table tb in db.Tables)
                {
                    // check if the table is not a system table
                    if (!tb.IsSystemObject && isTablaSemilla(tb.Name.ToString(), dt) && tb.Name.ToString() == dep.Urn.GetAttribute("Name"))
                    {
                        Scripter scrpt = new Scripter(srv);
                        scrpt.Options.ScriptData = true;
                        scrpt.Options.ScriptSchema = false;

                        var script = scrpt.EnumScript(new SqlSmoObject[] { tb });
                        foreach (var line in script)
                        {
                            builder.AppendLine(line);
                            contador += 1;

                            if ((contador % 100) == 0)
                                builder.AppendLine("GO"); // Se coloca GO cada 100 statements
                        }
                    }
                }
                contador = 0;
            }

            File.WriteAllText(path, builder.ToString());

            return true;
        }

        // Obtiene las tablas semilla de OptimusDB
        static DataTable getTablasSemilla() {
            DataTable dt = new DataTable();
            SqlDataAdapter adapter = new SqlDataAdapter();
            SqlConnection con = new SqlConnection("Server=" + ConfigurationManager.AppSettings["serverInstance"].ToString() + ";Database=" + ConfigurationManager.AppSettings["DB"].ToString() + ";User ID=" + ConfigurationManager.AppSettings["Login"].ToString() + ";Password=" + ConfigurationManager.AppSettings["Password"].ToString() + ";Trusted_Connection=False;");
            SqlCommand comando = new SqlCommand();

            try {
                comando.Connection = con;
                comando.CommandType = CommandType.Text;
                comando.CommandText = "select tabla from dbo.Semillas;";
                using (SqlDataAdapter da = new SqlDataAdapter(comando))
                {
                    da.Fill(dt);
                }
            }
            catch(Exception ex) {
                Console.WriteLine(ex.Message.ToString());
            }
            finally {
                con.Close();
            }

            return dt;
        }

        // Determina si la tabla es semilla o no
        static Boolean isTablaSemilla(string tabla, DataTable tablasSemilla) {
            foreach(DataRow dr in tablasSemilla.Rows){
                if (dr["tabla"].ToString() == tabla)
                    return true;            
            }

            return false;
        }
    }
}
