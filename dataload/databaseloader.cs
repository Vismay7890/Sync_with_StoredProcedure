using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using latlog;
using System.Diagnostics;

namespace Roughdb
{
    internal class databaseloader
    {
        public static string InferPrimaryKeyColumnName(DataTable schemaTable)
        {
            var primaryKeyColumn = schemaTable.Columns.Cast<DataColumn>().FirstOrDefault(c => c.Unique);

            return primaryKeyColumn != null ? primaryKeyColumn.ColumnName : null;
        }
        public bool TableExists(string connectionString, string tableName)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}_clone'";

                        int count = (int)command.ExecuteScalar();

                        Latlog.Log(LogLevel.Info, $"Table '{tableName}_clone' exists: {count > 0}");

                        return count > 0;
                    }
                }
            }
            catch (Exception ex)
            {
                Latlog.LogError("TableExists", $"Exception occured {ex.Message}", ex);
                return false;
            }
        }






        public DataTable GetUpdatedData(DataTable dataTable, string connectionString, string tableName, string primaryKeyColumn)
        {
            DataTable dbDataTable = LoadDataFromDatabase(connectionString, $"SELECT * FROM {tableName}_clone");
            dbDataTable.Columns.Remove("TimestampColumn");

            DataTable updatedDataTable = dbDataTable.Clone();

            foreach (DataRow newRow in dataTable.Rows)
            {
                DataRow[] databaseRows = dbDataTable.Select($"{primaryKeyColumn} = '{newRow[primaryKeyColumn]}'");

                if (databaseRows.Length > 0)
                {
                    DataRow databaseRow = databaseRows[0];
                    List<string> differences = new List<string>();

                    for (int i = 0; i < newRow.ItemArray.Length; i++)
                    {
                        string columnName = dataTable.Columns[i].ColumnName;

                        if (columnName != "TimestampColumn")
                        {
                            if (dataTable.Columns[i].DataType == typeof(DateTime))
                            {
                                DateTime newDate = Convert.ToDateTime(newRow[i]).Date;
                                DateTime dbDate = Convert.ToDateTime(databaseRow[i]).Date;

                                if (newDate != dbDate)
                                {
                                    differences.Add($"{columnName}: {dbDate:yyyy-MM-dd} => {newDate:yyyy-MM-dd}");
                                    databaseRow[i] = newRow[i];
                                }
                            }
                            else
                            {
                                string newValue = Convert.ToString(newRow[i]);
                                string dbValue = Convert.ToString(databaseRow[i]);

                                if (!string.Equals(newValue, dbValue))
                                {
                                    differences.Add($"{columnName}: {dbValue} => {newValue}");
                                    databaseRow[i] = newRow[i];
                                }
                            }
                        }
                    }

                    if (differences.Any())
                    {
                        updatedDataTable.ImportRow(databaseRow);
                    }
                }
                else
                {
                    // If the record is not found in the database, consider it as a new record
                    updatedDataTable.ImportRow(newRow);
                }
            }

            return updatedDataTable;
        }

        public void CreateTable(string connectionString, string tableName, DataTable schemaTable)
        {
            try
            {
                //DataTable schemaTable = LoadSchemaTable(tableName, schemaXmlFilePath);

                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand())
                    {
                        command.Connection = connection;

                        string createTableCommand = GenerateCreateTableCommand(tableName, schemaTable);

                        command.CommandText = createTableCommand;

                        command.ExecuteNonQuery();
                    }
                }

                Latlog.Log(LogLevel.Info, $"Table '{tableName}' created successfully.");
            }
            catch (Exception ex)
            {
                Latlog.LogError("CreateTable", $"Exception occured {ex.Message}", ex);
                //
                //Latlog.Log(LogLevel.Info,$"Exception Stack Trace: {ex.StackTrace}");
            }

        }
        public static string GenerateCreateTableCommand(string tableName, DataTable schemaTable)
        {
            try
            {
                StringBuilder createTableCommand = new StringBuilder();
                createTableCommand.Append($"CREATE TABLE {tableName}_clone (");


                List<string> columnDefinitions = new List<string>();
                List<string> constraints = new List<string>();

                foreach (DataColumn column in schemaTable.Columns)
                {
                    string columnName = column.ColumnName;
                    Type dataType = column.DataType;

                    // Build the column definition using the updated mapping
                    string columnDefinition = $"{columnName} {MapDataTypeToSqlType(dataType, column.Unique)}";

                    // Add debug statement to inspect the column definition
                    Console.WriteLine($"Column Definition: {columnDefinition}");
                    // Check for constraints
                    if (column.Unique)
                    {
                        Console.WriteLine("Primary Key is True");
                        constraints.Add($"CONSTRAINT PK_{tableName}_{columnName} PRIMARY KEY ({columnName})");
                        Console.WriteLine($"Added PK constraint for column: {columnName}");
                    }
                    else if (Convert.ToBoolean(column.ExtendedProperties["IS_FOREIGN_KEY"]))
                    {
                        string foreignKeyTable = column.ExtendedProperties["FOREIGN_KEY_TABLE"].ToString();
                        string foreignKeyColumn = column.ExtendedProperties["FOREIGN_KEY_COLUMN"].ToString();
                        constraints.Add($"CONSTRAINT FK_{tableName}_{columnName} FOREIGN KEY ({columnName}) REFERENCES {foreignKeyTable}({foreignKeyColumn})");
                        Console.WriteLine($"Added FK constraint for column: {columnName}");
                    }

                    columnDefinitions.Add(columnDefinition);
                }
                columnDefinitions.Add("TimestampColumn timestamp");

                Console.WriteLine($"Column Definitions: {string.Join(", ", columnDefinitions)}");

                createTableCommand.Append(string.Join(", ", columnDefinitions));

                // Append constraints only if there are any
                if (constraints.Count > 0)
                {
                    Console.WriteLine($"Constraints: {string.Join(", ", constraints)}");
                    createTableCommand.Append(", " + string.Join(", ", constraints));
                }

                createTableCommand.Append(");");

                Latlog.Log(LogLevel.Info, $"SQL Command: {createTableCommand}");

                return createTableCommand.ToString();
            }
            catch (Exception ex)
            {
                Latlog.LogError("GenerateCreateTableCommand", $"Exception occured {ex.Message}", ex);
                throw; // Rethrow the exception after logging
            }
        }

        public static string MapDataTypeToSqlType(Type dataType, bool isPrimaryKey = false)
        {
            if (isPrimaryKey)
            {
                // Handle special cases for primary key data types
                if (dataType == typeof(int))
                {
                    return "INT";
                }
                // Add more cases for other primary key data types as needed
            }

            // Generic mapping for other data types
            switch (Type.GetTypeCode(dataType))
            {
                case TypeCode.Int32:
                    return "INT";
                case TypeCode.String:
                    return "VARCHAR(MAX)";
                case TypeCode.DateTime:
                    return "DATE";
                case TypeCode.Char:
                    return "VARCHAR(MAX)";
                // Add more cases for other data types as needed
                default:
                    return "VARCHAR(MAX)";
            }
        }
        public static DataTable LoadDataFromDatabase(string connectionString, string query)
        {

            DataTable dataTable = new DataTable();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlDataAdapter adapter = new SqlDataAdapter(query, connection))
                    {
                        adapter.Fill(dataTable);
                    }
                }
                return dataTable;
            }
            catch (Exception ex)
            {
                //Console.WriteLine($"Error loading data from database in LoadDataFromDatabase: {ex.Message}");
                Latlog.LogError("LoadDataFromDatabase", $"Exception occured {ex.Message}", ex);
                return null;
            }
            finally
            {
                Latlog.Log(LogLevel.Info, "DataTable from Databaseloaded");

            }
        }
        public void InsertDataInSql(string connectionString,  DataTable SchemaTable, string targetTableName, string primarykeycolumn , DataTable dataTable)
        {
            Latlog.Log(LogLevel.Info ,"Entered Insertdatainsql function..");
            // Check and create stored procedure
            CreateStoredProcedure(connectionString, targetTableName , primarykeycolumn, SchemaTable , dataTable);

        }
        private static bool ProcedureExists(SqlConnection connection, string procedureName)
        {
            using (SqlCommand command = new SqlCommand("SELECT 1 FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = @ProcedureName", connection))
            {
                command.Parameters.AddWithValue("@ProcedureName", procedureName);
                return command.ExecuteScalar() != null;
            }
        }

        private static string GenerateCustomTableType(DataTable schemaTable , string tablename)
        {
            try
            {
                StringBuilder typeBuilder = new StringBuilder();
                typeBuilder.AppendLine($"IF NOT EXISTS (SELECT * FROM sys.types WHERE name = 'CustomTableType_{tablename}_clone') ");
                typeBuilder.AppendLine("BEGIN");
                typeBuilder.AppendLine($"    CREATE TYPE dbo.CustomTableType_{tablename}_clone AS TABLE (");

                List<string> columnDefinitions = new List<string>();

                foreach (DataColumn column in schemaTable.Columns)
                {
                    string columnName = column.ColumnName;
                    Type dataType = column.DataType;
                    bool isPrimaryKey = column.ExtendedProperties.Contains("IS_PRIMARY_KEY") && (bool)column.ExtendedProperties["IS_PRIMARY_KEY"];
                    bool isNullable = column.AllowDBNull;

                    // Build the column definition using the existing mapping
                    string columnDefinition = $"{columnName} {MapDataTypeToSqlType(dataType, isPrimaryKey)}";

                    if (!isNullable)
                    {
                        columnDefinition += " NOT NULL";
                    }


                    columnDefinitions.Add(columnDefinition);
                }

                typeBuilder.AppendLine(string.Join(", ", columnDefinitions));
                typeBuilder.AppendLine(");");
                typeBuilder.AppendLine("END");
                Latlog.Log(LogLevel.Info, $"Custom Table Type is:{typeBuilder}");

                return typeBuilder.ToString();
            }
            catch (Exception ex)
            {
                Latlog.LogError("GenerateCustomTableType", $"Exception occurred: {ex.Message}", ex);
                throw; // Rethrow the exception after logging
            }
        }


        public static void CreateStoredProcedure(string connectionString, string tableName, string primaryKeyColumn, DataTable schemaTable, DataTable dataTable)
        {

            Latlog.Log(LogLevel.Info, "Entered CreateStoredProccedure function..");

            using (SqlConnection con = new SqlConnection(connectionString))
            {
                try
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand();

                    // Check if the stored procedure already exists
                    if (!ProcedureExists(con, $"fullSync_{tableName}_clone"))
                    {
                        // Create the custom table type dynamically
                        string customTableTypeSql = GenerateCustomTableType(schemaTable , tableName);

                        // Create the custom table type first
                        cmd.CommandText = customTableTypeSql;
                        cmd.Connection = con;
                        cmd.ExecuteNonQuery();
                        Latlog.Log(LogLevel.Info, "Custom table type created successfully.");

                        // Now create the stored procedure dynamically
                        StringBuilder sqlBuilder = new StringBuilder();
                        sqlBuilder.AppendLine($"CREATE PROCEDURE fullSync_{tableName}_clone");
                        sqlBuilder.AppendLine($"@tblLog dbo.CustomTableType_{tableName}_clone READONLY");
                        sqlBuilder.AppendLine("AS");
                        sqlBuilder.AppendLine("BEGIN");
                        sqlBuilder.AppendLine("SET NOCOUNT ON;");
                        sqlBuilder.AppendLine($"MERGE INTO dbo.[{tableName}_clone] AS target");
                        sqlBuilder.AppendLine("USING @tblLog AS source");
                        sqlBuilder.AppendLine($"ON (target.{primaryKeyColumn} = source.{primaryKeyColumn})");
                        sqlBuilder.AppendLine("WHEN MATCHED THEN");
                        sqlBuilder.AppendLine("UPDATE SET ");

                        // Dynamic UPDATE SET clause
                        List<string> updateSetClauses = schemaTable.Columns
                         .Cast<DataColumn>()
                         .Where(c => c.ColumnName != primaryKeyColumn)
                         .Select(c => $"target.{c.ColumnName} = CASE WHEN source.{c.ColumnName} <> target.{c.ColumnName} THEN source.{c.ColumnName} ELSE target.{c.ColumnName} END")
                         .ToList();

                        if (updateSetClauses.Any())
                        {
                            sqlBuilder.AppendLine(string.Join(", ", updateSetClauses));
                        }
                        else
                        {
                            // If there are no columns to update, just append a dummy condition to avoid syntax issues
                            sqlBuilder.AppendLine("1 = 1");
                        }

                        sqlBuilder.AppendLine("WHEN NOT MATCHED THEN");
                        sqlBuilder.AppendLine("INSERT (");
                        sqlBuilder.AppendLine(string.Join(", ", schemaTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName)));
                        sqlBuilder.AppendLine(")");
                        sqlBuilder.AppendLine("VALUES (");
                        sqlBuilder.AppendLine(string.Join(", ", schemaTable.Columns.Cast<DataColumn>().Select(c => "source." + c.ColumnName)));
                        sqlBuilder.AppendLine(");");
                        sqlBuilder.AppendLine("END;");
                        
                        cmd.CommandText = sqlBuilder.ToString();


                        Latlog.Log(LogLevel.Info, $"Command text for Procedure is: {cmd.CommandText}");

                        cmd.ExecuteNonQuery();
                        Latlog.Log(LogLevel.Info, $"Stored procedure 'fullSync_{tableName}_clone' created successfully.");

                        // Now execute the stored procedure with your data
                        ExecuteStoredProcedure(connectionString, $"fullSync_{tableName}_clone", dataTable );
                    }
                    else
                    {
                        ExecuteStoredProcedure(connectionString, $"fullSync_{tableName}_clone", dataTable);
                        Latlog.Log(LogLevel.Info, $"Stored procedure 'fullSync_{tableName}_clone' already exists.");
                    }
                }
                catch (Exception ex)
                {
                    Latlog.LogError("CreateStoredProcedure", $"Exception occurred: {ex.Message}", ex);
                }
            }
        }


        private static void ExecuteStoredProcedure(string connectionString, string storedProcedureName, DataTable dataTable)
        {
            using (SqlConnection con = new SqlConnection(connectionString))
            {
                try
                {
                    con.Open();
                    SqlCommand cmd = new SqlCommand
                    {
                        Connection = con,
                        CommandType = CommandType.StoredProcedure,
                        CommandText = storedProcedureName
                    };
                    string tableName = storedProcedureName.Substring(storedProcedureName.IndexOf("_") + 1, storedProcedureName.LastIndexOf("_") - storedProcedureName.IndexOf("_") - 1);

                    // Add DataTable parameter to the stored procedure
                    var dataTableParam1 = new SqlParameter("@tblLog", SqlDbType.Structured)
                    {
                        TypeName = $"dbo.CustomTableType_{tableName}_clone", 
                        Value = dataTable
                    };
                  
                    cmd.Parameters.Add(dataTableParam1);

                    

                    // Execute the stored procedure
                    cmd.ExecuteNonQuery();
                    Latlog.Log(LogLevel.Info, $"Stored procedure '{storedProcedureName}' executed successfully.");
                }
                catch (Exception ex)
                {
                    Latlog.LogError("ExecuteStoredProcedure", $"Exception occurred: {ex.Message}", ex);
                }
            }
        }



        private static string GetPrimaryKeyColumn(string primaryKeyColumn)
        {
            return $"LT.{primaryKeyColumn}";
        }

        private static string GetWhereConditionColumn(DataTable schemaTable, string primaryKeyColumn)
        {
            return $"{GetPrimaryKeyColumn(primaryKeyColumn)} = OT.{primaryKeyColumn}";
        }

    }
}
