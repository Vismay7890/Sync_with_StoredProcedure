using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;
using System.Net.Http.Headers;
using Newtonsoft.Json;
using System.Net;
using Newtonsoft.Json.Linq;
using latlog;
using System.Configuration;
using System.Data.SqlClient;
using System.Data;
using System.Data.SqlTypes;
namespace Roughcrm
{
    internal class SalesforceClient
    {
        private const string LOGIN_ENDPOINT = "https://login.salesforce.com/services/oauth2/token";
        private const string API_ENDPOINT = "/services/data/v51.0";
        public string Username { get; set; }
        public string Password { get; set; }
        public string Token { get; set; }
        public string ClientId { get; set; }
        public string ClientSecret { get; set; }
        public string refresh_token { get; set; }

        public string AuthToken { get; set; }
        public string InstanceUrl { get; set; }

        static SalesforceClient()
        {
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12 | SecurityProtocolType.Tls;
        }

        public void login()
        {
            Latlog.Log(LogLevel.Info, "Login Function Invoked");

            try
            {
                var clientId = ClientId;
                var clientSecret = ClientSecret;
                var username = Username;
                var password = Password + Token;

                var client = new HttpClient();
                var tokenRequest = new HttpRequestMessage(HttpMethod.Post, LOGIN_ENDPOINT);
                tokenRequest.Content = new FormUrlEncodedContent(new[]
                {
            new KeyValuePair<string, string>("grant_type", "password"),
            new KeyValuePair<string, string>("client_id", clientId),
            new KeyValuePair<string, string>("client_secret", clientSecret),
            new KeyValuePair<string, string>("username", username),
            new KeyValuePair<string, string>("password", password)
        });

                // Request the token
                var tokenResponse = client.SendAsync(tokenRequest).Result;
                var body = tokenResponse.Content.ReadAsStringAsync().Result;

                if (!tokenResponse.IsSuccessStatusCode)
                {
                    Latlog.Log(LogLevel.Info, $"Error getting access token. Status Code: {tokenResponse.StatusCode}, Reason: {tokenResponse.ReasonPhrase}");
                    return;
                }

                var values = JsonConvert.DeserializeObject<Dictionary<string, string>>(body);

                if (values.ContainsKey("access_token"))
                {
                    AuthToken = values["access_token"];
                    Latlog.Log(LogLevel.Info, "AuthToken = " + AuthToken);
                }
                else
                {
                    Latlog.Log(LogLevel.Info, "Access token not found in the response.");
                    return;
                }

                if (values.ContainsKey("instance_url"))
                {
                    InstanceUrl = values["instance_url"];
                    Latlog.Log(LogLevel.Info, "Instance URL = " + InstanceUrl);
                }
                else
                {
                    Latlog.Log(LogLevel.Info, "Instance URL not found in the response.");
                }
            }
            catch (Exception ex)
            {
                Latlog.Log(LogLevel.Error, $"An error occurred during login: {ex.Message}");
            }
        }

        public void UpdateTimestampRepo(DataTable updateTable, DateTime UpsertTime , string tableName , string connectionString)
        {
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    // Create stored procedure
                    using (SqlCommand createCommand = new SqlCommand(GetInsertTimeRepoStoredProcedureScript(), connection))
                    {
                        createCommand.ExecuteNonQuery();
                    }

                    // Execute the stored procedure with TVP
                    using (SqlCommand executeCommand = new SqlCommand("InsertTimeRepo", connection))
                    {
                        executeCommand.CommandType = CommandType.StoredProcedure;

                        // Add TVP parameter
                        SqlParameter tvpParam = executeCommand.Parameters.AddWithValue("@DataTable", updateTable);
                        tvpParam.SqlDbType = SqlDbType.Structured;
                        tvpParam.TypeName = "dbo.TimeRepoTableType";

                        // Execute the stored procedure
                        int rowsAffected = executeCommand.ExecuteNonQuery();

                        Latlog.Log(LogLevel.Info, $"Rows affected: {rowsAffected}");
                    }
                }
            }
            catch (Exception ex)
            {
                Latlog.LogError("UpdateTimestampRepo", $"Error updating/inserting TimestampRepo:", ex);
            }
        }

        private string GetInsertTimeRepoStoredProcedureScript()
        {
            // Define the stored procedure script
            string script = @"
CREATE PROCEDURE InsertTimeRepo
(
    @DataTable TimeRepoTableType READONLY
)
AS
BEGIN
SET NOCOUNT ON
    MERGE INTO dbo.TimestampRepository AS target
    USING @DataTable AS source
ON (target.[Key] = 'Students_clone_VW:' + CAST(source.id AS VARCHAR(900)))    
WHEN MATCHED THEN
        UPDATE SET 
target.SavedTimeStamp = (SELECT TimestampColumn FROM Students_clone WHERE StudentID = source.id), target.UpSertTime= getdate()
WHEN NOT MATCHED THEN
INSERT (
[Key],SavedTimeStamp,SFDCID
)
VALUES (
('Students_clone_VW:' + CAST(source.id AS VARCHAR(900))), (SELECT TimestampColumn FROM Students_clone WHERE StudentID = source.id), source.SFDCID
);
END;

            ";

            return script;
        }



        // Add this method to your SalesforceClient class
        public void Upsert(string sObject, List<JObject> jsonObjects, string primaryKeyColumnName, string tableName, string connectionString)
        {
            const int batchSize = 200;

            try
            {
                using (var client = new HttpClient())
                {
                    DataTable updateTable = new DataTable();
                    updateTable.Columns.Add("SFDCID", typeof(string));
                    updateTable.Columns.Add("id", typeof(string));
                    for (int i = 0; i < jsonObjects.Count; i += batchSize)
                    {
                        var batchObjects = jsonObjects.Skip(i).Take(batchSize).ToList();
                        var batchRequest = new
                        {
                            allOrNone = false,
                            records = new List<object>()
                        };

                        foreach (var jsonObject in batchObjects)
                        {
                            string externalId = jsonObject.Value<string>(primaryKeyColumnName);

                            // Ensure the "attributes" section exists
                            if (jsonObject["attributes"] == null)
                            {
                                jsonObject["attributes"] = new JObject();
                            }

                            // Set type and external ID dynamically
                            jsonObject["attributes"]["type"] = sObject;
                            jsonObject["attributes"][primaryKeyColumnName] = externalId;

                            // Convert date fields to ISO 8601 format
                            foreach (var property in jsonObject.Properties())
                            {
                                if (property.Value.Type == JTokenType.Date)
                                {
                                    property.Value = ((DateTime)property.Value).ToString("yyyy-MM-ddTHH:mm:ss");
                                }
                            }

                            batchRequest.records.Add(jsonObject);
                            string studentId = jsonObject.Value<string>(primaryKeyColumnName);
                            string sfdcId = ""; 
                            updateTable.Rows.Add(sfdcId, studentId);
                        }

                        string restRequest = $"{InstanceUrl}{API_ENDPOINT}/composite/sobjects/{sObject}/{primaryKeyColumnName}";
                        //Latlog.Log(LogLevel.Info, $"REST Request URL For Updating  " + restRequest); // Debug statement

                        string batchRequestJson = JsonConvert.SerializeObject(batchRequest);
                        //Latlog.Log(LogLevel.Info, $"Batch Request Fields: {batchRequestJson}");

                        // Create the HTTP request
                        var request = new HttpRequestMessage(HttpMethod.Patch, restRequest);
                        request.Headers.Add("Authorization", "Bearer " + AuthToken);
                        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                        request.Headers.Add("X-PrettyPrint", "1");

                        // Set the request body with the updated JSON data
                        request.Content = new StringContent(batchRequestJson, Encoding.UTF8, "application/json");

                        // Send the request and get the response
                        var response = client.SendAsync(request).Result;

                        if (response.IsSuccessStatusCode)
                        {
                            JArray responseArray = JArray.Parse(response.Content.ReadAsStringAsync().Result);

                            // Iterate through each response object in the array
                            int j = 0;

                            // Iterate through each response object in the array
                            foreach (JObject responseObject in responseArray)
                            {

                                string sfdcId = responseObject.Value<string>("id");
                                updateTable.Rows[j]["SFDCID"] = sfdcId;
                                //Latlog.Log(LogLevel.Info, $"Record Upserted Successfully - SFDC ID: {sfdcId}");
                                j++;
                            }
                            UpdateTimestampRepo(updateTable, DateTime.Now, tableName, connectionString);
                        }
                        else
                        {
                            Latlog.Log(LogLevel.Error, $"Data Upsert Failed HTTP Status Code: {response.StatusCode}, Response: {response.Content.ReadAsStringAsync().Result}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Latlog.Log(LogLevel.Error, $"Exception in Upsert: {ex.Message}");
                // Handle the exception as needed
            }
        }

    }

    // Helper method to convert DataRow to JSON strin


}

