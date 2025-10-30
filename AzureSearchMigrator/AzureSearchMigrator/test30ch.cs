// Program.cs
using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace AzureSearchMigrator
{
    class Program
    {
        private static IConfiguration Configuration;

        static async Task Main(string[] args)
        {
            try
            {
                LoadConfiguration();

                var sourceServiceName = Configuration["SourceSearchService:ServiceName"];
                var sourceIndexName = Configuration["SourceSearchService:IndexName"];
                var sourceAdminApiKey = Configuration["SourceSearchService:AdminApiKey"];

                var targetServiceName = Configuration["TargetSearchService:ServiceName"];
                var targetIndexName = Configuration["TargetSearchService:IndexName"];
                var targetAdminApiKey = Configuration["TargetSearchService:AdminApiKey"];

                var batchSize = int.TryParse(Configuration["MigrationSettings:BatchSize"], out var bs) ? bs : 1000;
                var apiVersion = Configuration["MigrationSettings:ApiVersion"] ?? "2023-11-01";

                if (string.IsNullOrEmpty(sourceServiceName) || string.IsNullOrEmpty(sourceAdminApiKey) ||
                    string.IsNullOrEmpty(targetServiceName) || string.IsNullOrEmpty(targetAdminApiKey))
                {
                    Console.WriteLine("ERROR: Please update appsettings.json with your Azure Search service details.");
                    Console.WriteLine("Press any key to exit...");
                    Console.ReadKey();
                    return;
                }

                Console.WriteLine("Starting Azure Cognitive Search Migration...");
                Console.WriteLine($"Source: {sourceServiceName}/{sourceIndexName}");
                Console.WriteLine($"Target: {targetServiceName}/{targetIndexName}");
                Console.WriteLine($"Batch Size: {batchSize}");
                Console.WriteLine();

                Console.WriteLine("Step 1: Retrieving index schema from source...");
                var indexSchema = await GetIndexSchemaAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                if (indexSchema == null)
                {
                    Console.WriteLine("Failed to retrieve index schema. Exiting.");
                    return;
                }

                Console.WriteLine("Step 2: Creating index in target service...");
                await CreateTargetIndexAsync(targetServiceName, targetIndexName, targetAdminApiKey, indexSchema, apiVersion);

                Console.WriteLine("Step 3: Counting documents in source index...");
                int totalDocuments = await GetTotalDocumentCountViaApiAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                Console.WriteLine("Step 4: Migrating data using REST API...");
                await MigrateAllDocumentsViaRestApiAsync(
                    sourceServiceName, sourceIndexName, sourceAdminApiKey,
                    targetServiceName, targetIndexName, targetAdminApiKey,
                    batchSize, totalDocuments, apiVersion);

                Console.WriteLine("\nMigration completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        static void LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            Configuration = builder.Build();
        }

        static async Task<string> GetIndexSchemaAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) })
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        var schema = await response.Content.ReadAsStringAsync();
                        Console.WriteLine("Successfully retrieved index schema.");
                        return schema;
                    }
                    else
                    {
                        Console.WriteLine($"Failed to get index schema. Status: {response.StatusCode}");
                        return null;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error retrieving index schema: {ex.Message}");
                return null;
            }
        }

        static async Task CreateTargetIndexAsync(string serviceName, string indexName, string apiKey, string indexSchema, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(120) })
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var content = new StringContent(indexSchema, System.Text.Encoding.UTF8, "application/json");
                    var response = await httpClient.PutAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Successfully created target index.");
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to create target index. Status: {response.StatusCode}, Error: {error}");
                        throw new Exception($"Failed to create target index: {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating target index: {ex.Message}");
                throw;
            }
        }

        static async Task<int> GetTotalDocumentCountViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) })
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/$count?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        var countText = await response.Content.ReadAsStringAsync();
                        if (int.TryParse(countText, out int count))
                        {
                            return count;
                        }
                        else if (long.TryParse(countText, out long countLong))
                        {
                            return (int)countLong;
                        }
                    }
                    Console.WriteLine($"Failed to get document count via API. Status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count via API: {ex.Message}");
            }
            return 0;
        }

        // returns tuple: (keyFieldName, keyFieldType) where keyFieldType is the "type" from the schema (e.g. "Edm.String", "Edm.Int32")
        static async Task<(string name, string type)> GetKeyFieldNameViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) })
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        var schemaJson = await response.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(schemaJson))
                        {
                            if (doc.RootElement.TryGetProperty("fields", out var fieldsArray))
                            {
                                foreach (var field in fieldsArray.EnumerateArray())
                                {
                                    if (field.TryGetProperty("key", out var keyProperty) && keyProperty.GetBoolean())
                                    {
                                        var name = field.GetProperty("name").GetString();
                                        var type = field.GetProperty("type").GetString();
                                        Console.WriteLine($"Found key field in schema: {name} ({type})");
                                        return (name, type);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not determine key field from schema: {ex.Message}");
            }

            // Fallback: sample document
            try
            {
                using (var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) })
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?api-version={apiVersion}&$top=1&search=*";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(json))
                        {
                            if (doc.RootElement.TryGetProperty("value", out var valueArray) && valueArray.GetArrayLength() > 0)
                            {
                                var firstDoc = valueArray.EnumerateArray().First();
                                // Common key candidates
                                var possibleKeys = new[] { "id", "key", "documentid", "docid", "Id", "Key", "ID" };
                                foreach (var prop in firstDoc.EnumerateObject())
                                {
                                    if (possibleKeys.Contains(prop.Name.ToLower()))
                                    {
                                        Console.WriteLine($"Found key field from document sample: {prop.Name}");
                                        // unknown type - default to string
                                        return (prop.Name, "Edm.String");
                                    }
                                }
                                var fallbackKey = firstDoc.EnumerateObject().First().Name;
                                Console.WriteLine($"Using fallback key field: {fallbackKey}");
                                return (fallbackKey, "Edm.String");
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not determine key field from document: {ex.Message}");
            }

            Console.WriteLine("Using default key field name: 'id' (Edm.String)");
            return ("id", "Edm.String");
        }

        static async Task MigrateAllDocumentsViaRestApiAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string apiVersion)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            int batchNumber = 0;
            var startTime = DateTime.Now;

            try
            {
                Console.WriteLine($"Starting migration of {totalDocuments:N0} documents...");
                Console.WriteLine($"Batch size: {batchSize}");
                Console.WriteLine($"Estimated batches: {Math.Ceiling(totalDocuments / (double)batchSize)}");
                Console.WriteLine($"Start time: {startTime:yyyy-MM-dd HH:mm:ss}");
                Console.WriteLine(new string('=', 70));

                var (keyFieldName, keyFieldType) = await GetKeyFieldNameViaApiAsync(sourceServiceName, sourceIndexName, sourceApiKey, apiVersion);
                Console.WriteLine($"Using key field for pagination: {keyFieldName} ({keyFieldType})");

                string lastKey = null;
                bool hasMoreDocuments = true;
                int consecutiveEmptyBatches = 0;

                while (hasMoreDocuments && totalMigratedDocuments < totalDocuments)
                {
                    batchNumber++;

                    try
                    {
                        var documents = await GetDocumentsWithCursorAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            batchSize, keyFieldName, lastKey, apiVersion, keyFieldType);

                        if (documents == null || documents.Count == 0)
                        {
                            consecutiveEmptyBatches++;
                            Console.WriteLine($"Batch {batchNumber}: No documents returned (consecutive empty batches: {consecutiveEmptyBatches})");

                            if (consecutiveEmptyBatches >= 3)
                            {
                                Console.WriteLine("Too many consecutive empty batches - stopping migration");
                                hasMoreDocuments = false;
                                break;
                            }

                            await Task.Delay(2000);
                            continue;
                        }
                        else
                        {
                            consecutiveEmptyBatches = 0;
                        }

                        if (documents.Count > 0)
                        {
                            var lastDocument = documents.Last();
                            if (lastDocument.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                            {
                                lastKey = keyValue.ToString();
                                Console.WriteLine($"Last key in batch: {lastKey}");
                            }
                            else
                            {
                                Console.WriteLine($"Warning: Could not get key value from last document in batch");
                            }
                        }

                        bool batchSuccess = await IndexDocumentsViaApiAsync(
                            targetServiceName, targetIndexName, targetApiKey,
                            documents, apiVersion, keyFieldName);

                        if (batchSuccess)
                        {
                            successfulBatches++;
                            totalMigratedDocuments += documents.Count;

                            double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                            var elapsed = DateTime.Now - startTime;
                            var documentsPerSecond = totalMigratedDocuments / Math.Max(elapsed.TotalSeconds, 1);

                            var remainingDocuments = totalDocuments -
