using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;

namespace AzureSearchMigrator
{
    class Program
    {
        private static IConfiguration Configuration;

        static async Task Main(string[] args)
        {
            try
            {
                // Load configuration
                LoadConfiguration();

                // Get settings from configuration
                var sourceServiceName = Configuration["SourceSearchService:ServiceName"];
                var sourceIndexName = Configuration["SourceSearchService:IndexName"];
                var sourceAdminApiKey = Configuration["SourceSearchService:AdminApiKey"];

                var targetServiceName = Configuration["TargetSearchService:ServiceName"];
                var targetIndexName = Configuration["TargetSearchService:IndexName"];
                var targetAdminApiKey = Configuration["TargetSearchService:AdminApiKey"];

                // Use the configured batch size or default to 1000
                if (!int.TryParse(Configuration["MigrationSettings:BatchSize"], out int batchSize))
                {
                    batchSize = 1000;
                }
                var apiVersion = Configuration["MigrationSettings:ApiVersion"] ?? "2023-11-01";

                // Validate configuration
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

                // Step 1: Get index schema from source
                Console.WriteLine("Step 1: Retrieving index schema from source...");
                var indexSchema = await GetIndexSchemaAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                if (indexSchema == null)
                {
                    Console.WriteLine("Failed to retrieve index schema. Exiting.");
                    return;
                }

                // Step 2: Create index in target
                Console.WriteLine("Step 2: Creating index in target service...");
                await CreateTargetIndexAsync(targetServiceName, targetIndexName, targetAdminApiKey, indexSchema, apiVersion);

                // Step 3: Get total document count
                Console.WriteLine("Step 3: Counting documents in source index...");
                int totalDocuments = await GetTotalDocumentCountViaApiAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 4: Migrate all data using REST API with search pagination
                Console.WriteLine("Step 4: Migrating data using search pagination...");
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
                using (var httpClient = new HttpClient())
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
                        var error = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Error Details: {error}");
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
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    using var doc = JsonDocument.Parse(indexSchema);
                    var newSchema = RemoveReadOnlyProperties(doc.RootElement);
                    var content = new StringContent(newSchema, System.Text.Encoding.UTF8, "application/json");

                    var response = await httpClient.PutAsync(url, content);

                    if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.Created)
                    {
                        Console.WriteLine("Successfully created or updated target index.");
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

        static string RemoveReadOnlyProperties(JsonElement originalElement)
        {
            using var ms = new MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            foreach (var property in originalElement.EnumerateObject())
            {
                if (property.Name.StartsWith("@odata.", StringComparison.OrdinalIgnoreCase) ||
                    property.Name.Equals("eTag", StringComparison.OrdinalIgnoreCase) ||
                    property.Name.Equals("lastModified", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (property.Name.Equals("fields", StringComparison.OrdinalIgnoreCase) && property.Value.ValueKind == JsonValueKind.Array)
                {
                    writer.WriteStartArray(property.Name);
                    foreach (var fieldElement in property.Value.EnumerateArray())
                    {
                        writer.WriteStartObject();
                        foreach (var fieldProperty in fieldElement.EnumerateObject())
                        {
                            if (!fieldProperty.Name.Equals("@odata.type", StringComparison.OrdinalIgnoreCase))
                            {
                                fieldProperty.WriteTo(writer);
                            }
                        }
                        writer.WriteEndObject();
                    }
                    writer.WriteEndArray();
                }
                else
                {
                    property.WriteTo(writer);
                }
            }
            writer.WriteEndObject();
            writer.Flush();
            return Encoding.UTF8.GetString(ms.ToArray());
        }

        static async Task<int> GetTotalDocumentCountViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/stats?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        var statsJson = await response.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(statsJson))
                        {
                            if (doc.RootElement.TryGetProperty("documentCount", out var countElement) && countElement.ValueKind == JsonValueKind.Number)
                            {
                                return countElement.GetInt32();
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count via index statistics: {ex.Message}. Falling back to $count.");
            }

            try
            {
                using (var httpClient = new HttpClient())
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
                    }
                }
            }
            catch (Exception) { }

            return 0;
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

            Console.WriteLine($"Starting migration of {totalDocuments:N0} documents...");
            Console.WriteLine($"Batch size: {batchSize}");
            Console.WriteLine(new string('=', 70));

            string searchText = "*";
            string continuationToken = null;
            bool hasMoreDocuments = true;

            while (hasMoreDocuments)
            {
                batchNumber++;
                List<Dictionary<string, object>> documents = null;
                bool batchSuccess = false;

                try
                {
                    // Get documents using search with continuation
                    var result = await GetDocumentsWithSearchAsync(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        batchSize, searchText, continuationToken, apiVersion);

                    documents = result.documents;
                    continuationToken = result.continuationToken;

                    if (documents == null || documents.Count == 0)
                    {
                        Console.WriteLine("No more documents found.");
                        hasMoreDocuments = false;
                        break;
                    }

                    // Index the batch
                    batchSuccess = await IndexDocumentsViaApiAsync(
                        targetServiceName, targetIndexName, targetApiKey,
                        documents, apiVersion);

                    if (batchSuccess)
                    {
                        successfulBatches++;
                        totalMigratedDocuments += documents.Count;

                        double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                        var elapsed = DateTime.Now - startTime;
                        var documentsPerSecond = totalMigratedDocuments / Math.Max(elapsed.TotalSeconds, 1);

                        // Calculate ETA
                        var remainingDocuments = totalDocuments - totalMigratedDocuments;
                        var estimatedTimeRemaining = remainingDocuments / Math.Max(documentsPerSecond, 1);
                        var eta = TimeSpan.FromSeconds(estimatedTimeRemaining);

                        Console.WriteLine(
                            $"✓ Batch {batchNumber}: {documents.Count,4} docs - " +
                            $"Total: {totalMigratedDocuments,7:N0}/{totalDocuments,7:N0} ({percentage,5:F1}%) - " +
                            $"Speed: {documentsPerSecond,6:F1} doc/s - " +
                            $"ETA: {eta:hh\\:mm\\:ss}");
                    }
                    else
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Batch {batchNumber}: Failed to index documents. Will retry this batch.");
                        // Don't update continuationToken on failure - we'll retry the same batch
                        continuationToken = null;
                    }

                    // If no continuation token, we've reached the end
                    if (string.IsNullOrEmpty(continuationToken))
                    {
                        hasMoreDocuments = false;
                    }

                    // Small delay to avoid overwhelming the service
                    if (batchSuccess)
                    {
                        await Task.Delay(500);
                    }
                    else
                    {
                        // Longer delay on failure before retry
                        await Task.Delay(3000);
                    }
                }
                catch (Exception ex)
                {
                    failedBatches++;
                    Console.WriteLine($"✗ Critical error processing batch {batchNumber}: {ex.Message}. Pausing before next attempt.");
                    continuationToken = null; // Reset on critical error
                    await Task.Delay(5000);
                }

                // Safety limit
                if (batchNumber > (totalDocuments / batchSize) * 2 + 10)
                {
                    Console.WriteLine("Safety limit reached - stopping migration due to excessive retries or batches.");
                    break;
                }
            }

            // Final summary
            var totalTime = DateTime.Now - startTime;
            Console.WriteLine(new string('=', 70));
            Console.WriteLine("=== MIGRATION COMPLETE ===");
            Console.WriteLine($"Total documents in source (initial estimate): {totalDocuments:N0}");
            Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
            int missingCount = totalDocuments - totalMigratedDocuments;
            Console.WriteLine($"Discrepancy: {missingCount:N0}");
            Console.WriteLine($"Successful batches: {successfulBatches}");
            Console.WriteLine($"Failed batches (retried/skipped): {failedBatches}");
            Console.WriteLine($"Total batches processed: {batchNumber}");
            Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");

            if (totalTime.TotalSeconds > 0)
            {
                double overallSpeed = totalMigratedDocuments / totalTime.TotalSeconds;
                Console.WriteLine($"Overall speed: {overallSpeed:F2} documents/second");
            }

            if (missingCount != 0)
            {
                Console.WriteLine($"\n⚠ Warning: There's a discrepancy of {missingCount:N0} documents. This could be due to documents being added/removed during migration or count inaccuracies.");
            }
        }

        static async Task<(List<Dictionary<string, object>> documents, string continuationToken)> GetDocumentsWithSearchAsync(
            string serviceName, string indexName, string apiKey,
            int top, string searchText, string continuationToken, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var searchRequest = new
                    {
                        search = searchText,
                        top = top,
                        queryType = "simple",
                        includeTotalResultCount = true
                    };

                    // Add continuation token if we have one
                    if (!string.IsNullOrEmpty(continuationToken))
                    {
                        url += $"&continuationToken={Uri.EscapeDataString(continuationToken)}";
                    }

                    var content = new StringContent(JsonSerializer.Serialize(searchRequest), Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        var documents = new List<Dictionary<string, object>>();
                        string nextContinuationToken = null;

                        using (var doc = JsonDocument.Parse(json))
                        {
                            // Extract documents
                            if (doc.RootElement.TryGetProperty("value", out var valueArray))
                            {
                                foreach (var element in valueArray.EnumerateArray())
                                {
                                    var documentDict = new Dictionary<string, object>();
                                    foreach (var prop in element.EnumerateObject())
                                    {
                                        if (prop.Name.StartsWith("@search.") || prop.Name.StartsWith("@odata.")) 
                                            continue;

                                        documentDict[prop.Name] = GetValueFromJsonElement(prop.Value);
                                    }
                                    documents.Add(documentDict);
                                }
                            }

                            // Extract continuation token for next page
                            if (doc.RootElement.TryGetProperty("@odata.nextLink", out var nextLinkElement))
                            {
                                var nextLink = nextLinkElement.GetString();
                                var uri = new Uri(nextLink);
                                var queryParams = System.Web.HttpUtility.ParseQueryString(uri.Query);
                                nextContinuationToken = queryParams["continuationToken"];
                            }
                        }

                        return (documents, nextContinuationToken);
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to search documents: {response.StatusCode}, Error: {errorContent}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error searching documents via API: {ex.Message}");
            }
            return (new List<Dictionary<string, object>>(), null);
        }

        static async Task<bool> IndexDocumentsViaApiAsync(
            string serviceName, string indexName, string apiKey,
            List<Dictionary<string, object>> documents, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/index?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var indexOperations = documents.Select(doc =>
                    {
                        var operation = new Dictionary<string, object>(doc)
                        {
                            { "@search.action", "upload" } 
                        };
                        return operation;
                    }).ToList();

                    var indexRequest = new
                    {
                        value = indexOperations 
                    };

                    var content = new StringContent(JsonSerializer.Serialize(indexRequest), Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        return true;
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to index documents: {response.StatusCode}. Details: {error}");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error indexing documents via API: {ex.Message}");
                return false;
            }
        }

        static object GetValueFromJsonElement(JsonElement element)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.String:
                    return element.GetString();
                case JsonValueKind.Number:
                    if (element.TryGetInt32(out int intValue))
                        return intValue;
                    if (element.TryGetInt64(out long longValue))
                        return longValue;
                    return element.GetDouble();
                case JsonValueKind.True:
                    return true;
                case JsonValueKind.False:
                    return false;
                case JsonValueKind.Null:
                    return null;
                case JsonValueKind.Array:
                    var array = new List<object>();
                    foreach (var item in element.EnumerateArray())
                    {
                        array.Add(GetValueFromJsonElement(item));
                    }
                    return array;
                case JsonValueKind.Object:
                    var dict = new Dictionary<string, object>();
                    foreach (var prop in element.EnumerateObject())
                    {
                        dict[prop.Name] = GetValueFromJsonElement(prop.Value);
                    }
                    return dict;
                default:
                    return element.ToString();
            }
        }
    }
}
