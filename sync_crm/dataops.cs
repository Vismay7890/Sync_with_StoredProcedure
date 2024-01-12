using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using latlog;

namespace Roughcrm
{
    internal class dataop
    {
        public string InferPrimaryKeyColumnNameFromDatabase(string connectionString, string tableName)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // Retrieve primary key column from INFORMATION_SCHEMA.COLUMNS
                    string query = $@"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tableName}' AND COLUMN_NAME IN (
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = '{tableName}' AND CONSTRAINT_NAME LIKE 'PK_%'
                )";

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            if (reader.Read())
                            {
                                // Get the primary key column name
                                string primaryKeyColumn = reader["COLUMN_NAME"].ToString();

                                // Add '__c' suffix to the primary key column name
                                return $"{primaryKeyColumn}__c";
                            }
                        }
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inferring primary key column: {ex.Message}");
                return null;
            }
        }


        public DataTable FetchStudentDataIntoDataTable(string connectionString, string query)
        {
            DataTable dataTable = new DataTable();

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    using (SqlDataAdapter dataAdapter = new SqlDataAdapter(command))
                    {
                        dataAdapter.Fill(dataTable);
                    }
                }
            }
            if (dataTable.Columns.Contains("TimestampColumn"))
            {
                dataTable.Columns.Remove("TimestampColumn");
            }
            if (dataTable.Columns.Contains("SavedTimestamp"))
            {
                dataTable.Columns.Remove("SavedTimestamp");
            }
            return dataTable;
        }
        public string AddSuffixToColumnNames(string jsonData, string suffix)
        {

            JArray jsonArray = JArray.Parse(jsonData);

            foreach (JObject item in jsonArray)
            {
                var properties = item.Properties().ToList();

                foreach (var property in properties)
                {
                    if (property.Name == "FirstName")
                    {

                        item.Add(new JProperty("Name", property.Value));
                        item.Remove(property.Name);
                    }

                    else
                    {
                        item.Remove(property.Name);
                        item.Add(new JProperty(property.Name + suffix, property.Value));
                    }
                }
            }

            return jsonArray.ToString();
        }
        public string CreateView(string connectionString, string tableName, string primarykeycolumn)
        {
            string PK = primarykeycolumn.TrimEnd('_').TrimEnd('c').TrimEnd('_');
            Latlog.Log(LogLevel.Info, "Primarykeycolumn:" + PK);
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();

                string view = $"CREATE VIEW [dbo].[{tableName}_VW] AS SELECT T.SavedTimeStamp, ";

                var columns = GetColumnNames(tableName, connection);
                foreach (string column in columns)
                {
                    view += $"G.{column} AS {column}, ";
                }

                // Remove the extra comma at the end of the SELECT list
                view = view.TrimEnd(',', ' ');

                view += $" FROM {tableName} G LEFT JOIN dbo.TimestampRepository AS T ON '{tableName}_VW:' + CAST(G.{PK} AS VARCHAR(255))  = T.[Key] WHERE (T.SavedTimeStamp IS NULL OR G.TimestampColumn != T.SavedTimeStamp);";

                Latlog.Log(LogLevel.Info, $"View query: {view}");


                using (var command = new SqlCommand(view, connection))
                {
                    Latlog.Log(LogLevel.Info, "View creation query being fired");
                    command.ExecuteNonQuery();
                }

                Latlog.Log(LogLevel.Info, $"View {tableName}_VW created...");
                return view;
            }
        }

        static List<string> GetColumnNames(string tableName, SqlConnection connection)
        {
            List<string> columnNames = new List<string>();
            string query = $"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tableName}'";

            using (var command = new SqlCommand(query, connection))
            {
                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader["COLUMN_NAME"].ToString();
                        columnNames.Add(columnName);
                    }
                }
            }

            return columnNames;
        }

    }
}
