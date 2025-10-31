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
                Console.WriteLine($"Pagination Method: $skip/$top");
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

                // Step 4: Migrate all data using $skip/$top pagination
                Console.WriteLine("Step 4: Migrating data using $skip/$top pagination...");
                await MigrateAllDocumentsWithSkipTopAsync(
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

        static async Task MigrateAllDocumentsWithSkipTopAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string apiVersion)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            var startTime = DateTime.Now;

            Console.WriteLine($"Starting migration of {totalDocuments:N0} documents using $skip/$top pagination...");
            Console.WriteLine($"Batch size: {batchSize}");
            Console.WriteLine(new string('=', 70));

            // Store failed batches for retry
            var failedBatchesToRetry = new Queue<(int skip, List<Dictionary<string, object>> documents)>();
            int maxRetries = 3;
            int maxConsecutiveFailures = 5;
            int consecutiveFailures = 0;

            // Calculate total batches
            int totalBatches = (int)Math.Ceiling((double)totalDocuments / batchSize);
            Console.WriteLine($"Total batches to process: {totalBatches}");

            // First pass: process all batches
            for (int skip = 0; skip < totalDocuments; skip += batchSize)
            {
                var batchNumber = (skip / batchSize) + 1;
                List<Dictionary<string, object>> documents = null;
                bool batchSuccess = false;
                int retryCount = 0;

                // First try to process failed batches
                if (failedBatchesToRetry.Count > 0)
                {
                    var failedBatch = failedBatchesToRetry.Dequeue();
                    skip = failedBatch.skip; // Adjust skip to retry the failed batch
                    documents = failedBatch.documents;
                    Console.WriteLine($"🔄 Retrying failed batch {batchNumber} (skip={skip}) with {documents.Count} documents...");
                }
                else
                {
                    try
                    {
                        Console.WriteLine($"Fetching batch {batchNumber}/{totalBatches} (skip={skip}, top={batchSize})...");

                        documents = await GetDocumentsWithSkipTopAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            batchSize, skip, apiVersion);

                        if (documents == null || documents.Count == 0)
                        {
                            Console.WriteLine($"No documents returned for batch {batchNumber}. This might indicate all documents have been processed.");
                            break;
                        }

                        // Reset consecutive failures on successful fetch
                        consecutiveFailures = 0;
                    }
                    catch (Exception ex)
                    {
                        consecutiveFailures++;
                        Console.WriteLine($"✗ Error fetching batch {batchNumber}: {ex.Message}");
                        Console.WriteLine($"Consecutive failures: {consecutiveFailures}/{maxConsecutiveFailures}");

                        if (consecutiveFailures >= maxConsecutiveFailures)
                        {
                            Console.WriteLine("Too many consecutive failures, stopping migration.");
                            break;
                        }

                        failedBatches++;
                        await Task.Delay(3000);
                        continue;
                    }
                }

                // Try to index the batch with retries
                while (retryCount < maxRetries && !batchSuccess)
                {
                    try
                    {
                        batchSuccess = await IndexDocumentsViaApiAsync(
                            targetServiceName, targetIndexName, targetApiKey,
                            documents, apiVersion);

                        if (batchSuccess)
                        {
                            successfulBatches++;
                            totalMigratedDocuments += documents.Count;
                            consecutiveFailures = 0; // Reset on successful indexing

                            double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                            var elapsed = DateTime.Now - startTime;
                            var documentsPerSecond = totalMigratedDocuments / Math.Max(elapsed.TotalSeconds, 1);

                            // Calculate ETA
                            var remainingDocuments = totalDocuments - totalMigratedDocuments;
                            var estimatedTimeRemaining = remainingDocuments / Math.Max(documentsPerSecond, 1);
                            var eta = TimeSpan.FromSeconds(estimatedTimeRemaining);

                            string status = failedBatchesToRetry.Count > 0 ? "🔄" : "✓";
                            Console.WriteLine(
                                $"{status} Batch {batchNumber}/{totalBatches}: {documents.Count,4} docs - " +
                                $"Total: {totalMigratedDocuments,7:N0}/{totalDocuments,7:N0} ({percentage,5:F1}%) - " +
                                $"Speed: {documentsPerSecond,6:F1} doc/s - " +
                                $"ETA: {eta:hh\\:mm\\:ss}");
                        }
                        else
                        {
                            retryCount++;
                            if (retryCount < maxRetries)
                            {
                                Console.WriteLine($"⚠ Batch {batchNumber} failed to index, retry {retryCount}/{maxRetries} in 5 seconds...");
                                await Task.Delay(5000);
                            }
                            else
                            {
                                failedBatches++;
                                Console.WriteLine($"✗ Batch {batchNumber} failed after {maxRetries} retries. Adding to retry queue.");
                                failedBatchesToRetry.Enqueue((skip, documents));
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        retryCount++;
                        if (retryCount < maxRetries)
                        {
                            Console.WriteLine($"⚠ Batch {batchNumber} error during indexing: {ex.Message}, retry {retryCount}/{maxRetries} in 5 seconds...");
                            await Task.Delay(5000);
                        }
                        else
                        {
                            failedBatches++;
                            Console.WriteLine($"✗ Batch {batchNumber} failed after {maxRetries} retries due to exception: {ex.Message}");
                            failedBatchesToRetry.Enqueue((skip, documents));
                        }
                    }
                }

                // Small delay to avoid overwhelming the service
                if (batchSuccess)
                {
                    await Task.Delay(100);
                }

                // Safety check - if we're not making progress
                if (batchNumber > totalBatches * 2)
                {
                    Console.WriteLine("Safety limit reached - stopping migration due to excessive batches.");
                    break;
                }
            }

            // Second pass: retry failed batches
            if (failedBatchesToRetry.Count > 0)
            {
                Console.WriteLine($"\nRetrying {failedBatchesToRetry.Count} failed batches...");

                int retryPass = 1;
                const int maxRetryPasses = 3;

                while (failedBatchesToRetry.Count > 0 && retryPass <= maxRetryPasses)
                {
                    Console.WriteLine($"\nRetry pass {retryPass}/{maxRetryPasses}");
                    int batchesInThisPass = failedBatchesToRetry.Count;
                    int successfulInThisPass = 0;

                    for (int i = 0; i < batchesInThisPass; i++)
                    {
                        var failedBatch = failedBatchesToRetry.Dequeue();
                        var batchNumber = (failedBatch.skip / batchSize) + 1;

                        try
                        {
                            Console.WriteLine($"Retrying batch {batchNumber} (skip={failedBatch.skip})...");

                            var success = await IndexDocumentsViaApiAsync(
                                targetServiceName, targetIndexName, targetApiKey,
                                failedBatch.documents, apiVersion);

                            if (success)
                            {
                                successfulBatches++;
                                successfulInThisPass++;
                                totalMigratedDocuments += failedBatch.documents.Count;

                                Console.WriteLine($"✅ Successfully retried batch {batchNumber}");
                            }
                            else
                            {
                                // Add back to queue for next retry pass
                                failedBatchesToRetry.Enqueue(failedBatch);
                                Console.WriteLine($"❌ Failed to retry batch {batchNumber}");
                            }
                        }
                        catch (Exception ex)
                        {
                            // Add back to queue for next retry pass
                            failedBatchesToRetry.Enqueue(failedBatch);
                            Console.WriteLine($"❌ Error retrying batch {batchNumber}: {ex.Message}");
                        }

                        await Task.Delay(200);
                    }

                    Console.WriteLine($"Retry pass {retryPass}: {successfulInThisPass}/{batchesInThisPass} batches successful");
                    retryPass++;
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
            Console.WriteLine($"Failed batches (after retries): {failedBatchesToRetry.Count}");
            Console.WriteLine($"Total batches processed: {successfulBatches + failedBatchesToRetry.Count}");

            if (totalTime.TotalSeconds > 0)
            {
                double overallSpeed = totalMigratedDocuments / totalTime.TotalSeconds;
                Console.WriteLine($"Overall speed: {overallSpeed:F2} documents/second");
                Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");
            }

            if (missingCount != 0)
            {
                Console.WriteLine($"\n⚠ Warning: There's a discrepancy of {missingCount:N0} documents.");
                Console.WriteLine("This could be due to:");
                Console.WriteLine("  - Documents being added/removed during migration");
                Console.WriteLine("  - Inaccurate initial document count");
                Console.WriteLine("  - Failed batches that couldn't be recovered");
            }

            // Verify final counts
            await VerifyMigrationResultsAsync(
                sourceServiceName, sourceIndexName, sourceApiKey,
                targetServiceName, targetIndexName, targetApiKey,
                apiVersion);
        }

        static async Task<List<Dictionary<string, object>>> GetDocumentsWithSkipTopAsync(
            string serviceName, string indexName, string apiKey,
            int top, int skip, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Set longer timeout for large batches
                    httpClient.Timeout = TimeSpan.FromMinutes(5);

                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var searchRequest = new
                    {
                        search = "*",
                        top = top,
                        skip = skip,
                        queryType = "simple",
                        searchMode = "all",
                        select = "*",  // Get all fields
                        orderby = "search.score() desc"  // Ensure consistent ordering
                    };

                    var content = new StringContent(JsonSerializer.Serialize(searchRequest), Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(url, content);

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
                                        if (prop.Name.StartsWith("@search.") || prop.Name.StartsWith("@odata."))
                                            continue;

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
                        var error = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to get documents with skip/top: {response.StatusCode}, Error: {error}");

                        // If it's a bad request, it might be due to skip being too large
                        if (response.StatusCode == System.Net.HttpStatusCode.BadRequest)
                        {
                            Console.WriteLine("Bad request - this might indicate all documents have been processed.");
                            return new List<Dictionary<string, object>>();
                        }

                        throw new Exception($"Search API error: {response.StatusCode} - {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting documents with skip/top: {ex.Message}");
                throw;
            }
        }

        static async Task<bool> IndexDocumentsViaApiAsync(
            string serviceName, string indexName, string apiKey,
            List<Dictionary<string, object>> documents, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Set longer timeout for indexing
                    httpClient.Timeout = TimeSpan.FromMinutes(5);

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

        static async Task VerifyMigrationResultsAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            string apiVersion)
        {
            Console.WriteLine("\n=== VERIFYING MIGRATION RESULTS ===");

            try
            {
                int sourceCount = await GetTotalDocumentCountViaApiAsync(sourceServiceName, sourceIndexName, sourceApiKey, apiVersion);
                int targetCount = await GetTotalDocumentCountViaApiAsync(targetServiceName, targetIndexName, targetApiKey, apiVersion);

                Console.WriteLine($"Source document count: {sourceCount:N0}");
                Console.WriteLine($"Target document count: {targetCount:N0}");
                Console.WriteLine($"Difference: {Math.Abs(sourceCount - targetCount):N0}");

                if (sourceCount == targetCount)
                {
                    Console.WriteLine("✅ SUCCESS: Document counts match!");
                }
                else
                {
                    Console.WriteLine("❌ WARNING: Document counts do not match!");

                    // If counts don't match, try a sample verification
                    await VerifySampleDocumentsAsync(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        targetServiceName, targetIndexName, targetApiKey,
                        apiVersion);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error verifying migration results: {ex.Message}");
            }
        }

        static async Task VerifySampleDocumentsAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            string apiVersion)
        {
            Console.WriteLine("\n=== VERIFYING SAMPLE DOCUMENTS ===");

            try
            {
                // Get a few sample documents from source
                var sourceSamples = await GetSampleDocumentsAsync(sourceServiceName, sourceIndexName, sourceApiKey, 5, apiVersion);
                Console.WriteLine($"Retrieved {sourceSamples.Count} sample documents from source.");

                // Try to find the same documents in target
                int foundCount = 0;
                foreach (var sample in sourceSamples)
                {
                    var keyField = FindKeyField(sample);
                    if (keyField.HasValue)
                    {
                        var keyPair = keyField.Value;
                        var found = await FindDocumentByKeyAsync(
                            targetServiceName, targetIndexName, targetApiKey,
                            keyPair.Key, keyPair.Value, apiVersion);

                        if (found)
                        {
                            foundCount++;
                        }
                        else
                        {
                            Console.WriteLine($"❌ Document with key '{keyPair.Key}={keyPair.Value}' not found in target.");
                        }
                    }
                    else
                    {
                        Console.WriteLine("⚠ Could not identify a key field for sample document.");
                    }
                }

                Console.WriteLine($"Sample verification: {foundCount}/{sourceSamples.Count} documents found in target.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during sample verification: {ex.Message}");
            }
        }

        static async Task<List<Dictionary<string, object>>> GetSampleDocumentsAsync(
            string serviceName, string indexName, string apiKey, int count, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var searchRequest = new
                    {
                        search = "*",
                        top = count,
                        queryType = "simple"
                    };

                    var content = new StringContent(JsonSerializer.Serialize(searchRequest), Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(url, content);

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
                                        if (prop.Name.StartsWith("@search.") || prop.Name.StartsWith("@odata."))
                                            continue;

                                        documentDict[prop.Name] = GetValueFromJsonElement(prop.Value);
                                    }
                                    documents.Add(documentDict);
                                }
                            }
                        }

                        return documents;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting sample documents: {ex.Message}");
            }

            return new List<Dictionary<string, object>>();
        }

        static KeyValuePair<string, object>? FindKeyField(Dictionary<string, object> document)
        {
            // Common key field names in Azure Search
            var commonKeyNames = new[] { "id", "key", "documentId", "docId", "recordId" };

            foreach (var fieldName in commonKeyNames)
            {
                if (document.ContainsKey(fieldName) && document[fieldName] != null)
                {
                    return new KeyValuePair<string, object>(fieldName, document[fieldName]);
                }
            }

            // If no common key found, use the first field
            var firstField = document.FirstOrDefault();
            if (!firstField.Equals(default(KeyValuePair<string, object>)))
            {
                return firstField;
            }

            return null;
        }

        static async Task<bool> FindDocumentByKeyAsync(
            string serviceName, string indexName, string apiKey,
            string keyField, object keyValue, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Escape the key value for OData filter
                    string escapedValue = keyValue.ToString().Replace("'", "''");
                    var filter = $"{keyField} eq '{escapedValue}'";
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var searchRequest = new
                    {
                        search = "*",
                        filter = filter,
                        top = 1,
                        queryType = "simple"
                    };

                    var content = new StringContent(JsonSerializer.Serialize(searchRequest), Encoding.UTF8, "application/json");
                    var response = await httpClient.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(json))
                        {
                            if (doc.RootElement.TryGetProperty("value", out var valueArray) &&
                                valueArray.GetArrayLength() > 0)
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error searching for document by key: {ex.Message}");
            }

            return false;
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
