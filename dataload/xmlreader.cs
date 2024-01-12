using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using latlog;

namespace Roughdb
{
    internal class xmlreader
    {
        public DataTable LoadSchemaTable(string tableName, string schemaXmlFilePath)
        {
            DataTable schemaTable = new DataTable();

            try
            {
                XmlDocument schemaXmlDocument = new XmlDocument();
                schemaXmlDocument.Load(schemaXmlFilePath);

                //Console.WriteLine($"Schema XML: {schemaXmlDocument.OuterXml}");
                XmlElement rootElement = schemaXmlDocument.DocumentElement;
                // Change the XPath to match the actual structure of your XML
                //XmlNodeList enrollmentNodes = schemaXmlDocument.SelectNodes($"//DocumentElement/Students");

                // Console.WriteLine($"Number of Enrollment Nodes: {enrollmentNodes.Count}");

                foreach (XmlNode schemaNode in rootElement.ChildNodes)
                {
                    //Console.WriteLine($"Enrollment Node XML: {enrollmentNode.OuterXml}");
                    if (schemaNode.NodeType == XmlNodeType.Element)
                    {
                        // Extract column information
                        string columnName = schemaNode.SelectSingleNode("COLUMN_NAME")?.InnerText;
                        string dataType = schemaNode.SelectSingleNode("DATA_TYPE")?.InnerText;
                        string isNullable = schemaNode.SelectSingleNode("IS_NULLABLE")?.InnerText;
                        string isPrimaryKey = schemaNode.SelectSingleNode("IS_PRIMARY_KEY")?.InnerText;
                        string isForeignKey = schemaNode.SelectSingleNode("IS_FOREIGN_KEY")?.InnerText;

                        // Create DataColumn with correct data type and constraints
                        DataColumn column = new DataColumn(columnName, MapDataType(dataType));

                        if (isNullable?.ToLower() == "no")
                        {
                            column.AllowDBNull = false;
                        }

                        if (isPrimaryKey?.ToLower() == "true")
                        {
                            //Console.WriteLine("Checked specifically for primary being true");
                            column.Unique = true;
                            column.ExtendedProperties["IS_PRIMARY_KEY"] = true;

                        }

                        if (isForeignKey?.ToLower() == "true")
                        {
                            column.ExtendedProperties["IS_FOREIGN_KEY"] = true;
                            column.ExtendedProperties["FOREIGN_KEY_TABLE"] = schemaNode.SelectSingleNode("./FOREIGN_KEY_TABLE")?.InnerText;
                            column.ExtendedProperties["FOREIGN_KEY_COLUMN"] = schemaNode.SelectSingleNode("./FOREIGN_KEY_COLUMN")?.InnerText;
                        }
                        // Add DataColumn to the DataTable
                        schemaTable.Columns.Add(column);

                    }
                }



                Latlog.Log(LogLevel.Info, "Schema table loaded successfully.");
            }
            catch (Exception ex)
            {
                Latlog.LogError("LoadSchemaTable", $"Exception occured {ex.Message}", ex);
            }

            return schemaTable;
        }


        public static Type MapDataType(string dataType)
        {
            switch (dataType.ToLower())
            {
                case "int":
                    return typeof(int);
                case "varchar":
                    return typeof(string);
                case "nvarchar":
                    return typeof(string);
                case "date":
                    return typeof(DateTime);
                case "char":
                    return typeof(string);
                default:
                    return null;
            }
        }

       
        public DataTable LoadDataTable(string tableName, string dataXmlFilePath, DataTable schemaTable)
        {
            DataTable dataTable = new DataTable();

            try
            {
                XmlDocument dataXmlDocument = new XmlDocument();
                dataXmlDocument.Load(dataXmlFilePath);

                XmlElement rootElement = dataXmlDocument.DocumentElement;

                // Clear columns and add columns based on the schemaTable
                dataTable.Columns.Clear();
                foreach (DataColumn column in schemaTable.Columns)
                {
                    dataTable.Columns.Add(column.ColumnName, column.DataType);
                }
                //dataTable.Columns.Add("TimestampColumn");

                // Iterate over child nodes directly
                foreach (XmlNode elementNode in rootElement.ChildNodes)
                {
                    if (elementNode.NodeType == XmlNodeType.Element)
                    {
                        //Console.WriteLine($"Processing Element: {elementNode.Name}");

                        DataRow newRow = dataTable.NewRow();

                        foreach (DataColumn column in dataTable.Columns)
                        {
                            string columnName = column.ColumnName;
                            string columnValue = GetInnerText(elementNode, columnName);

                            newRow[columnName] = Convert.ChangeType(columnValue, column.DataType);
                            //          newRow["TimestampColumn"] = Convert.ToString(DateTime.Now);

                            // Print values being added to the DataTable for debugging
                            // Console.WriteLine($"{columnName}: {newRow[columnName]}");
                        }

                        dataTable.Rows.Add(newRow);
                    }
                }

                Latlog.Log(LogLevel.Info, "Data table loaded successfully.");
            }
            catch (Exception ex)
            {
                Latlog.LogError("LoadDataTable", $"Exception occurred {ex.Message}", ex);
            }

            return dataTable;
        }



        public static string GetInnerText(XmlNode node, string childNodeName)
        {
            XmlNode childNode = node.SelectSingleNode(childNodeName);
            return childNode?.InnerText ?? string.Empty;
        }
    }
}
