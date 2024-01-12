using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using latlog;
using Roughcrm;

namespace Roughcrm
{
    internal class Program
    {
        private static SalesforceClient CreateClient()
        {
            return new SalesforceClient
            {
                Username = ConfigurationManager.AppSettings["username"],
                Password = ConfigurationManager.AppSettings["password"],
                Token = ConfigurationManager.AppSettings["token"],
                ClientId = ConfigurationManager.AppSettings["clientId"],
                ClientSecret = ConfigurationManager.AppSettings["clientSecret"]
            };
        }

        public static void Main()
        {
            string connectionString = ConfigurationManager.AppSettings["ConnectionString"];
            string tableNames = ConfigurationManager.AppSettings["TableNames"];
            string customObjects = ConfigurationManager.AppSettings["CustomObjects"];
            var client = CreateClient();
            dataop dp = new dataop();

            // Assuming TableNames and CustomObjects are comma-separated lists

            char[] delimiter = { ',' };
            var tableNameArray = tableNames.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
            var customObjectArray = customObjects.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);

            try
            {
                client.login();

                if (tableNameArray.Length != customObjectArray.Length)
                {
                    Latlog.Log(LogLevel.Error, "Mismatched number of table names and custom objects.");
                    return;
                }

                for (int i = 0; i < tableNameArray.Length; i++)
                {
                    string tableName = tableNameArray[i].Trim();
                    string customObject = customObjectArray[i].Trim();

                    string viewName = $"[dbo].[{tableName}_VW]";

                    string primaryKeyColumnName = dp.InferPrimaryKeyColumnNameFromDatabase(connectionString, tableName);

                    using (SqlConnection connection = new SqlConnection(connectionString))
                    {
                        connection.Open();

                        using (SqlCommand command = new SqlCommand($"SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '{tableName}_VW'", connection))
                        {
                            Latlog.Log(LogLevel.Info, $"Checking for existence of view: {viewName}");
                            bool viewExists = (int)command.ExecuteScalar() > 0;

                            if (!viewExists)
                            {
                                Latlog.Log(LogLevel.Info, $"View {viewName} doesn't exist, creating new view...");
                                // Create the view if it doesn't exist
                                dp.CreateView(connectionString, tableName, primaryKeyColumnName);
                            }
                        }
                    }

                    string viewQuery = $"SELECT * FROM {viewName}";
                    DataTable syncDataTable = dp.FetchStudentDataIntoDataTable(connectionString, viewQuery);

                    if (syncDataTable.Rows.Count > 0)
                    {
                        Console.WriteLine($"Data has to be updated for {customObject}");

                        //string query = $"SELECT * FROM {viewName}";
                        //DataTable dt = dp.FetchStudentDataIntoDataTable(connectionString, query);

                        string jsonData = JsonConvert.SerializeObject(syncDataTable, Formatting.Indented);

                        string dataJson = dp.AddSuffixToColumnNames(jsonData, "__c");


                        if (dataJson != null)
                        {
                            JArray jsonArray = JArray.Parse(dataJson);
                            const int batchSize = 200;
                            List<JObject> batchObjects = new List<JObject>();

                            foreach (JObject item in jsonArray)
                            {
                                Console.WriteLine();

                                // Add the modified object to the batch
                                batchObjects.Add(item);

                                // Check if the batch size is reached
                                if (batchObjects.Count >= batchSize)
                                {
                                    // Upsert the batch
                                    client.Upsert(customObject, batchObjects, primaryKeyColumnName, tableName, connectionString);

                                    // Clear the batch for the next set of records
                                    batchObjects.Clear();
                                }
                            }

                            // Upsert any remaining records in the last batch
                            if (batchObjects.Count > 0)
                            {
                                client.Upsert(customObject, batchObjects, primaryKeyColumnName, tableName, connectionString);
                            }

                        }
                    }
                    else
                    {
                        Latlog.Log(LogLevel.Info, $"No Data To be Inserted Or Updated for {customObject}..");
                    }
                }
            }
            catch (Exception ex)
            {
                Latlog.LogError("Main", "Exception", ex);
            }
        }
    }
}
