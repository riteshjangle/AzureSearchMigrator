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
                // Using index stats is more reliable for large indexes
                int totalDocuments = await GetTotalDocumentCountViaApiAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 4: Migrate all data using REST API directly
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
                    // URL will attempt to create the index if it doesn't exist, or replace it if it does.
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    // Modify the index schema to remove service-specific properties like @odata.etag
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

        // Helper to remove properties that cause issues during index creation
        static string RemoveReadOnlyProperties(JsonElement originalElement)
        {
            using var ms = new MemoryStream();
            using var writer = new Utf8JsonWriter(ms);

            writer.WriteStartObject();
            foreach (var property in originalElement.EnumerateObject())
            {
                // List of properties to skip when creating a new index
                if (property.Name.StartsWith("@odata.") ||
                    property.Name.Equals("eTag", StringComparison.OrdinalIgnoreCase) ||
                    property.Name.Equals("suggesterName", StringComparison.OrdinalIgnoreCase) || // Remove if you don't use suggest
                    property.Name.Equals("lastModified", StringComparison.OrdinalIgnoreCase) ||
                    property.Name.Equals("suggesters", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                // Special handling for the 'fields' array to remove @odata.type
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

        // CORRECTED: Using index statistics for a more reliable document count
        static async Task<int> GetTotalDocumentCountViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Use the index statistics endpoint to get the document count
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/stats?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);
                    if (response.IsSuccessStatusCode)
                    {
                        var statsJson = await response.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(statsJson))
                        {
                            // The property name for document count in stats is 'documentCount'
                            if (doc.RootElement.TryGetProperty("documentCount", out var countElement) && countElement.ValueKind == JsonValueKind.Number)
                            {
                                int count = countElement.GetInt32();
                                return count;
                            }
                        }
                    }
                    Console.WriteLine($"Failed to get index statistics. Status: {response.StatusCode}. Falling back to $count if stats fails.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count via index statistics: {ex.Message}. Falling back to $count.");
            }

            // Fallback to the $count query if stats fail
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
                    Console.WriteLine($"Failed to get document count via $count query. Status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count via $count query: {ex.Message}");
            }

            return 0;
        }

        static async Task<string> GetKeyFieldNameViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Get index schema to find the key field
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
                                        return field.GetProperty("name").GetString();
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

            // Fallback logic from the original code remains a decent last resort
            return "id";
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

                // Get the key field name for cursor-based pagination
                string keyFieldName = await GetKeyFieldNameViaApiAsync(sourceServiceName, sourceIndexName, sourceApiKey, apiVersion);
                Console.WriteLine($"Using key field for pagination: {keyFieldName}");

                string lastKey = null;
                bool hasMoreDocuments = true;

                while (hasMoreDocuments && totalMigratedDocuments < totalDocuments)
                {
                    batchNumber++;
                    List<Dictionary<string, object>> documents = null;
                    bool batchSuccess = false;

                    try
                    {
                        // Step A: Get documents using cursor-based pagination
                        documents = await GetDocumentsWithCursorAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            batchSize, keyFieldName, lastKey, apiVersion);

                        if (documents == null || documents.Count == 0)
                        {
                            Console.WriteLine("No more documents found.");
                            hasMoreDocuments = false;
                            break;
                        }

                        // Step B: Index the batch
                        batchSuccess = await IndexDocumentsViaApiAsync(
                            targetServiceName, targetIndexName, targetApiKey,
                            documents, apiVersion);

                        if (batchSuccess)
                        {
                            successfulBatches++;
                            totalMigratedDocuments += documents.Count;

                            // CRITICAL FIX: Advance the cursor ONLY on success
                            if (documents.Count > 0)
                            {
                                var lastDocument = documents.Last();
                                if (lastDocument.ContainsKey(keyFieldName))
                                {
                                    // The key field value is the cursor for the next batch
                                    lastKey = lastDocument[keyFieldName]?.ToString();
                                }
                            }

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
                            // lastKey is NOT updated, forcing a retry of the same batch.
                        }

                        // Check if we got fewer documents than requested (end of data)
                        if (documents.Count < batchSize)
                        {
                            hasMoreDocuments = false;
                        }

                        // Small delay to avoid overwhelming the service
                        if (batchNumber % 10 == 0 && batchSuccess)
                        {
                            await Task.Delay(500);
                        }
                        else if (!batchSuccess)
                        {
                            // Longer delay on failure before retry
                            await Task.Delay(3000);
                        }
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Critical error processing batch {batchNumber}: {ex.Message}. Pausing before next attempt.");
                        // Force a retry of the batch after a significant delay
                        await Task.Delay(5000);
                    }

                    // Safety check against infinite loops/excessive retries (e.g., more than double the expected batches)
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
                Console.WriteLine($"Total documents in source: {totalDocuments:N0}");
                Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Missing documents: {totalDocuments - totalMigratedDocuments:N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Failed batches (retried/skipped): {failedBatches}");
                Console.WriteLine($"Total batches processed: {batchNumber}");
                Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");

                if (totalTime.TotalSeconds > 0)
                {
                    double overallSpeed = totalMigratedDocuments / totalTime.TotalSeconds;
                    Console.WriteLine($"Overall speed: {overallSpeed:F2} documents/second");
                }

                if (totalDocuments > 0 && totalMigratedDocuments < totalDocuments)
                {
                    Console.WriteLine($"\n⚠ Warning: Only {totalMigratedDocuments:N0} out of {totalDocuments:N0} documents were migrated. Check failed batches for details.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed completely: {ex.Message}");
                throw;
            }
        }

        static async Task<List<Dictionary<string, object>>> GetDocumentsWithCursorAsync(
            string serviceName, string indexName, string apiKey,
            int top, string keyFieldName, string lastKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Build the filter for cursor-based pagination: keyFieldName greater than the last key processed
                    string filter = null;
                    if (!string.IsNullOrEmpty(lastKey))
                    {
                        // Escape the lastKey for OData and create the filter
                        filter = $"{keyFieldName} gt '{EscapeODataString(lastKey)}'";
                    }

                    // Always order by the key field ascending for cursor pagination
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?api-version={apiVersion}" +
                              $"&$top={top}&$orderby={Uri.EscapeDataString(keyFieldName)} asc&search=*";

                    if (!string.IsNullOrEmpty(filter))
                    {
                        url += $"&$filter={Uri.EscapeDataString(filter)}";
                    }

                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        var documents = new List<Dictionary<string, object>>();

                        using (var doc = JsonDocument.Parse(json))
                        {
                            if (doc.RootElement.TryGetProperty("value", out var valueArray))
                            {
                                foreach (var element in valueArray.EnumerateArray())
                                {
                                    var documentDict = new Dictionary<string, object>();
                                    foreach (var prop in element.EnumerateObject())
                                    {
                                        // Skip Azure Search metadata properties
                                        if (prop.Name.StartsWith("@odata.")) continue;

                                        documentDict[prop.Name] = GetValueFromJsonElement(prop.Value);
                                    }
                                    documents.Add(documentDict);
                                }
                            }
                        }
                        return documents;
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to get documents: {response.StatusCode}, Error: {errorContent}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting documents via API: {ex.Message}");
            }
            return new List<Dictionary<string, object>>();
        }

        static string EscapeODataString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            // Simple OData string escaping - replace single quotes with two single quotes
            return value.Replace("'", "''");
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

                    // Map documents to index operations
                    var operations = documents.Select(doc => new
                    {
                        // "upload" is the index action for migration
                        // "key" is the property for the action
                        action = "upload",
                        // All properties of the document are automatically added here
                        // Use doc as the direct object for the upload operation
                        // Note: If your key field is named differently, Azure Search will still find it
                        // but it must be present in the document.
                        @search = doc
                    }).ToList();

                    var indexRequest = new
                    {
                        value = operations
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
                        // Log specific errors from the search service response body
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
                    // Preserve number precision by checking type
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
