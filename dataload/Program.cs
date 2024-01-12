using System;
using System.Configuration;
using System.Data;
using latlog;
using Roughdb;
using System.IO;
using Roughdb;

class MainDriver
{
    static void Main()
    {
        try
        {
            databaseloader dbload = new databaseloader();
            xmlreader xmlreader = new xmlreader();
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            string tableNamesString = ConfigurationManager.AppSettings["TableNames"];
            string[] tableNames = tableNamesString.Split(',');

            string xmlFolderPath = ConfigurationManager.AppSettings["XmlFolderPath"];

            Latlog.Log(LogLevel.Info, "Program started.");

            foreach (string tableName in tableNames)
            {
                // Get all XML files for the current table in the specified folder
                string[] dataAndSchemaFiles = Directory.GetFiles(xmlFolderPath, $"{tableName}_*.xml");

                // Separate data and schema files
                string dataFile = dataAndSchemaFiles.FirstOrDefault(f => f.EndsWith("_data.xml"));
                string schemaFile = dataAndSchemaFiles.FirstOrDefault(f => f.EndsWith("_tableType.xml"));
                Console.WriteLine($"Processing files for table {tableName}:");
                Console.WriteLine($"Data file: {dataFile}");
                Console.WriteLine($"Schema file: {schemaFile}");
                // Process if both data and schema files are found
                if (!string.IsNullOrEmpty(dataFile) && !string.IsNullOrEmpty(schemaFile))
                {
                    DataTable schemaTable = xmlreader.LoadSchemaTable(tableName, schemaFile);
                    string primaryKeyColumnName = databaseloader.InferPrimaryKeyColumnName(schemaTable);

                    if (!dbload.TableExists(connectionString, tableName))
                    {
                        dbload.CreateTable(connectionString, tableName, schemaTable);
                    }

                    DataTable dataTable = xmlreader.LoadDataTable(tableName, dataFile, schemaTable);
                    DataTable Rough = dbload.GetUpdatedData(dataTable, connectionString, tableName, primaryKeyColumnName);

                    foreach(DataRow row in Rough.Rows)
                    {
                        Console.WriteLine(string.Join(", ", row.ItemArray.Select(item => item.ToString())));
                    }
                    dbload.InsertDataInSql(connectionString, schemaTable, tableName , primaryKeyColumnName , Rough);

                    //Latlog.Log(LogLevel.Info, $"Table {tableName}_clone created and data inserted for {Path.GetFileName(dataFile)}.");
                }
                else
                {
                    Latlog.Log(LogLevel.Error, $"Skipping table creation for {tableName}_clone. Data or schema file not found.");
                }
            }

            Latlog.Log(LogLevel.Info, "Program completed successfully.");
        }
        catch (Exception ex)
        {
            Latlog.Log(LogLevel.Error, $"Exception occurred: {ex.Message}");
        }
    }
}








