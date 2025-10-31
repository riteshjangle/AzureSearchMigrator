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

                // Use the configured batch size or default to 500 (smaller for large indexes)
                if (!int.TryParse(Configuration["MigrationSettings:BatchSize"], out int batchSize))
                {
                    batchSize = 500;
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

                // Step 4: Choose migration strategy based on document count
                if (totalDocuments <= 100000)
                {
                    // Use $skip/$top for smaller indexes
                    Console.WriteLine("Step 4: Using $skip/$top pagination (index size <= 100,000 documents)...");
                    await MigrateWithSkipTopAsync(
                        sourceServiceName, sourceIndexName, sourceAdminApiKey,
                        targetServiceName, targetIndexName, targetAdminApiKey,
                        batchSize, totalDocuments, apiVersion);
                }
                else
                {
                    // Use range queries for larger indexes
                    Console.WriteLine("Step 4: Using range query pagination (index size > 100,000 documents)...");
                    await MigrateWithRangeQueriesAsync(
                        sourceServiceName, sourceIndexName, sourceAdminApiKey,
                        targetServiceName, targetIndexName, targetAdminApiKey,
                        batchSize, totalDocuments, apiVersion);
                }

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

        // Method for indexes <= 100,000 documents
        static async Task MigrateWithSkipTopAsync(
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

            var failedBatchesToRetry = new Queue<(int skip, List<Dictionary<string, object>> documents)>();
            int maxRetries = 3;

            int totalBatches = (int)Math.Ceiling((double)totalDocuments / batchSize);
            Console.WriteLine($"Total batches to process: {totalBatches}");

            for (int skip = 0; skip < totalDocuments; skip += batchSize)
            {
                var batchNumber = (skip / batchSize) + 1;
                List<Dictionary<string, object>> documents = null;
                bool batchSuccess = false;
                int retryCount = 0;

                // Process failed batches first
                if (failedBatchesToRetry.Count > 0)
                {
                    var failedBatch = failedBatchesToRetry.Dequeue();
                    skip = failedBatch.skip;
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
                            Console.WriteLine($"No documents returned for batch {batchNumber}. Stopping migration.");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"✗ Error fetching batch {batchNumber}: {ex.Message}");
                        failedBatches++;
                        await Task.Delay(3000);
                        continue;
                    }
                }

                // Index the batch with retries
                while (retryCount < maxRetries && !batchSuccess)
                {
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

                if (batchSuccess)
                {
                    await Task.Delay(100);
                }
            }

            // Final summary
            await PrintMigrationSummary(startTime, totalDocuments, totalMigratedDocuments, successfulBatches, failedBatches);

            // Verify results
            await VerifyMigrationResultsAsync(
                sourceServiceName, sourceIndexName, sourceApiKey,
                targetServiceName, targetIndexName, targetApiKey,
                apiVersion);
        }

        // Method for indexes > 100,000 documents using range queries
        static async Task MigrateWithRangeQueriesAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string apiVersion)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            var startTime = DateTime.Now;

            Console.WriteLine($"Starting migration of {totalDocuments:N0} documents using range query pagination...");
            Console.WriteLine($"Batch size: {batchSize}");
            Console.WriteLine(new string('=', 70));

            // First, identify a suitable field for range queries
            var keyField = await IdentifyKeyFieldForPagingAsync(sourceServiceName, sourceIndexName, sourceApiKey, apiVersion);
            if (string.IsNullOrEmpty(keyField))
            {
                Console.WriteLine("❌ Could not find a suitable field for range query pagination. Falling back to continuation tokens.");
                await MigrateWithContinuationTokensAsync(
                    sourceServiceName, sourceIndexName, sourceApiKey,
                    targetServiceName, targetIndexName, targetApiKey,
                    batchSize, totalDocuments, apiVersion);
                return;
            }

            Console.WriteLine($"Using field '{keyField}' for range query pagination.");

            // Get the min and max values for the key field
            var (minValue, maxValue) = await GetKeyFieldRangeAsync(sourceServiceName, sourceIndexName, sourceApiKey, keyField, apiVersion);
            if (minValue == null || maxValue == null)
            {
                Console.WriteLine("❌ Could not determine range of key field. Falling back to continuation tokens.");
                await MigrateWithContinuationTokensAsync(
                    sourceServiceName, sourceIndexName, sourceApiKey,
                    targetServiceName, targetIndexName, targetApiKey,
                    batchSize, totalDocuments, apiVersion);
                return;
            }

            Console.WriteLine($"Key field range: {minValue} to {maxValue}");

            // Use range queries to paginate through documents
            var failedBatchesToRetry = new Queue<(object startValue, List<Dictionary<string, object>> documents)>();
            int maxRetries = 3;
            object currentStart = minValue;
            int batchNumber = 0;

            while (currentStart != null)
            {
                batchNumber++;
                List<Dictionary<string, object>> documents = null;
                bool batchSuccess = false;
                int retryCount = 0;

                // Process failed batches first
                if (failedBatchesToRetry.Count > 0)
                {
                    var failedBatch = failedBatchesToRetry.Dequeue();
                    currentStart = failedBatch.startValue;
                    documents = failedBatch.documents;
                    Console.WriteLine($"🔄 Retrying failed batch {batchNumber} starting at {keyField}={currentStart} with {documents.Count} documents...");
                }
                else
                {
                    try
                    {
                        Console.WriteLine($"Fetching batch {batchNumber} starting at {keyField}={currentStart}...");

                        var result = await GetDocumentsWithRangeQueryAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            keyField, currentStart, batchSize, apiVersion);

                        documents = result.documents;
                        currentStart = result.nextStartValue;

                        if (documents == null || documents.Count == 0)
                        {
                            Console.WriteLine("No documents returned. Stopping migration.");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"✗ Error fetching batch {batchNumber}: {ex.Message}");
                        failedBatches++;
                        await Task.Delay(3000);
                        continue;
                    }
                }

                // Index the batch with retries
                while (retryCount < maxRetries && !batchSuccess)
                {
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

                        var remainingDocuments = totalDocuments - totalMigratedDocuments;
                        var estimatedTimeRemaining = remainingDocuments / Math.Max(documentsPerSecond, 1);
                        var eta = TimeSpan.FromSeconds(estimatedTimeRemaining);

                        string status = failedBatchesToRetry.Count > 0 ? "🔄" : "✓";
                        var lastKeyValue = documents.Count > 0 ? documents.Last().GetValueOrDefault(keyField, "N/A") : "N/A";

                        Console.WriteLine(
                            $"{status} Batch {batchNumber}: {documents.Count,4} docs - " +
                            $"Total: {totalMigratedDocuments,7:N0}/{totalDocuments,7:N0} ({percentage,5:F1}%) - " +
                            $"Speed: {documentsPerSecond,6:F1} doc/s - " +
                            $"ETA: {eta:hh\\:mm\\:ss} - " +
                            $"Current: {keyField}={lastKeyValue}");
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
                            failedBatchesToRetry.Enqueue((currentStart, documents));
                        }
                    }
                }

                if (batchSuccess && currentStart != null)
                {
                    await Task.Delay(100);
                }

                // Safety check
                if (batchNumber > (totalDocuments / Math.Max(batchSize, 1)) * 3)
                {
                    Console.WriteLine("Safety limit reached - stopping migration due to excessive batches.");
                    break;
                }
            }

            // Final summary
            await PrintMigrationSummary(startTime, totalDocuments, totalMigratedDocuments, successfulBatches, failedBatches);

            // Verify results
            await VerifyMigrationResultsAsync(
                sourceServiceName, sourceIndexName, sourceApiKey,
                targetServiceName, targetIndexName, targetApiKey,
                apiVersion);
        }

        // Fallback method using continuation tokens
        static async Task MigrateWithContinuationTokensAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string apiVersion)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            var startTime = DateTime.Now;

            Console.WriteLine("Using continuation token pagination...");
            Console.WriteLine($"Batch size: {batchSize}");
            Console.WriteLine(new string('=', 70));

            string continuationToken = null;
            bool hasMoreDocuments = true;
            var failedBatchesToRetry = new Queue<(string continuationToken, List<Dictionary<string, object>> documents)>();
            int maxRetries = 3;
            int batchNumber = 0;

            while (hasMoreDocuments || failedBatchesToRetry.Count > 0)
            {
                batchNumber++;
                List<Dictionary<string, object>> documents = null;
                bool batchSuccess = false;
                int retryCount = 0;
                string currentContinuationToken = continuationToken;

                // Process failed batches first
                if (failedBatchesToRetry.Count > 0)
                {
                    var failedBatch = failedBatchesToRetry.Dequeue();
                    currentContinuationToken = failedBatch.continuationToken;
                    documents = failedBatch.documents;
                    Console.WriteLine($"🔄 Retrying failed batch {batchNumber} with {documents.Count} documents...");
                }
                else if (hasMoreDocuments)
                {
                    try
                    {
                        Console.WriteLine($"Fetching batch {batchNumber} with continuation token...");

                        var result = await GetDocumentsWithContinuationTokenAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            batchSize, currentContinuationToken, apiVersion);

                        documents = result.documents;
                        continuationToken = result.continuationToken;

                        if (documents == null || documents.Count == 0)
                        {
                            Console.WriteLine("No documents returned. Stopping migration.");
                            hasMoreDocuments = false;
                            continue;
                        }

                        hasMoreDocuments = !string.IsNullOrEmpty(continuationToken);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"✗ Error fetching batch {batchNumber}: {ex.Message}");
                        failedBatches++;
                        await Task.Delay(3000);
                        continue;
                    }
                }
                else
                {
                    break;
                }

                // Index the batch with retries
                while (retryCount < maxRetries && !batchSuccess)
                {
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

                        var remainingDocuments = totalDocuments - totalMigratedDocuments;
                        var estimatedTimeRemaining = remainingDocuments / Math.Max(documentsPerSecond, 1);
                        var eta = TimeSpan.FromSeconds(estimatedTimeRemaining);

                        string status = failedBatchesToRetry.Count > 0 ? "🔄" : "✓";
                        Console.WriteLine(
                            $"{status} Batch {batchNumber}: {documents.Count,4} docs - " +
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
                            failedBatchesToRetry.Enqueue((currentContinuationToken, documents));
                        }
                    }
                }

                if (batchSuccess)
                {
                    await Task.Delay(100);
                }
            }

            // Final summary
            await PrintMigrationSummary(startTime, totalDocuments, totalMigratedDocuments, successfulBatches, failedBatches);

            // Verify results
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

        static async Task<string> IdentifyKeyFieldForPagingAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                // Get sample documents to identify a good key field
                var samples = await GetSampleDocumentsAsync(serviceName, indexName, apiKey, 10, apiVersion);

                foreach (var sample in samples)
                {
                    // Prefer string fields that look like IDs
                    var potentialKeys = sample.Where(kvp =>
                        kvp.Value is string &&
                        kvp.Key.ToLower().Contains("id") &&
                        !string.IsNullOrEmpty(kvp.Value as string))
                        .ToList();

                    if (potentialKeys.Any())
                    {
                        return potentialKeys.First().Key;
                    }

                    // Fall back to any string field
                    var stringField = sample.FirstOrDefault(kvp => kvp.Value is string && !string.IsNullOrEmpty(kvp.Value as string));
                    if (!stringField.Equals(default(KeyValuePair<string, object>)))
                    {
                        return stringField.Key;
                    }

                    // Fall back to any numeric field
                    var numericField = sample.FirstOrDefault(kvp => kvp.Value is int || kvp.Value is long);
                    if (!numericField.Equals(default(KeyValuePair<string, object>)))
                    {
                        return numericField.Key;
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error identifying key field: {ex.Message}");
                return null;
            }
        }

        static async Task<(object minValue, object maxValue)> GetKeyFieldRangeAsync(string serviceName, string indexName, string apiKey, string keyField, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    // Get min value
                    var minRequest = new
                    {
                        search = "*",
                        top = 1,
                        orderby = $"{keyField} asc",
                        select = keyField
                    };

                    var minContent = new StringContent(JsonSerializer.Serialize(minRequest), Encoding.UTF8, "application/json");
                    var minResponse = await httpClient.PostAsync(url, minContent);

                    if (minResponse.IsSuccessStatusCode)
                    {
                        var minJson = await minResponse.Content.ReadAsStringAsync();
                        using (var doc = JsonDocument.Parse(minJson))
                        {
                            if (doc.RootElement.TryGetProperty("value", out var valueArray) && valueArray.GetArrayLength() > 0)
                            {
                                var firstDoc = valueArray.EnumerateArray().First();
                                if (firstDoc.TryGetProperty(keyField, out var minField))
                                {
                                    var minValue = GetValueFromJsonElement(minField);

                                    // Get max value
                                    var maxRequest = new
                                    {
                                        search = "*",
                                        top = 1,
                                        orderby = $"{keyField} desc",
                                        select = keyField
                                    };

                                    var maxContent = new StringContent(JsonSerializer.Serialize(maxRequest), Encoding.UTF8, "application/json");
                                    var maxResponse = await httpClient.PostAsync(url, maxContent);

                                    if (maxResponse.IsSuccessStatusCode)
                                    {
                                        var maxJson = await maxResponse.Content.ReadAsStringAsync();
                                        using (var maxDoc = JsonDocument.Parse(maxJson))
                                        {
                                            if (maxDoc.RootElement.TryGetProperty("value", out var maxValueArray) && maxValueArray.GetArrayLength() > 0)
                                            {
                                                var lastDoc = maxValueArray.EnumerateArray().First();
                                                if (lastDoc.TryGetProperty(keyField, out var maxField))
                                                {
                                                    var maxValue = GetValueFromJsonElement(maxField);
                                                    return (minValue, maxValue);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting key field range: {ex.Message}");
            }

            return (null, null);
        }

        static async Task<(List<Dictionary<string, object>> documents, object nextStartValue)> GetDocumentsWithRangeQueryAsync(
            string serviceName, string indexName, string apiKey,
            string keyField, object startValue, int top, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/search?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    // Build filter for range query
                    string filter;
                    if (startValue is string)
                    {
                        filter = $"{keyField} ge '{startValue.ToString().Replace("'", "''")}'";
                    }
                    else if (startValue is int || startValue is long)
                    {
                        filter = $"{keyField} ge {startValue}";
                    }
                    else
                    {
                        filter = $"{keyField} ge {startValue}";
                    }

                    var searchRequest = new
                    {
                        search = "*",
                        top = top,
                        orderby = $"{keyField} asc",
                        filter = filter,
                        select = "*"
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

                        // Determine next start value
                        object nextStartValue = null;
                        if (documents.Count > 0)
                        {
                            var lastDocument = documents.Last();
                            if (lastDocument.ContainsKey(keyField))
                            {
                                nextStartValue = lastDocument[keyField];
                            }
                        }

                        return (documents, nextStartValue);
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        throw new Exception($"Range query failed: {response.StatusCode} - {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in range query: {ex.Message}");
                throw;
            }
        }

        static async Task<(List<Dictionary<string, object>> documents, string continuationToken)> GetDocumentsWithContinuationTokenAsync(
            string serviceName, string indexName, string apiKey,
            int top, string continuationToken, string apiVersion)
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
                        top = top,
                        queryType = "simple",
                        searchMode = "all",
                        select = "*"
                    };

                    var content = new StringContent(JsonSerializer.Serialize(searchRequest), Encoding.UTF8, "application/json");

                    if (!string.IsNullOrEmpty(continuationToken))
                    {
                        httpClient.DefaultRequestHeaders.Remove("continuationToken");
                        httpClient.DefaultRequestHeaders.Add("continuationToken", continuationToken);
                    }

                    var response = await httpClient.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        var documents = new List<Dictionary<string, object>>();
                        string nextContinuationToken = null;

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

                        if (response.Headers.TryGetValues("continuationToken", out var tokenValues))
                        {
                            nextContinuationToken = tokenValues.FirstOrDefault();
                        }

                        return (documents, nextContinuationToken);
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        throw new Exception($"Continuation token query failed: {response.StatusCode} - {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in continuation token query: {ex.Message}");
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

        static async Task PrintMigrationSummary(DateTime startTime, int totalDocuments, int totalMigratedDocuments, int successfulBatches, int failedBatches)
        {
            var totalTime = DateTime.Now - startTime;
            Console.WriteLine(new string('=', 70));
            Console.WriteLine("=== MIGRATION SUMMARY ===");
            Console.WriteLine($"Total documents in source: {totalDocuments:N0}");
            Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
            Console.WriteLine($"Discrepancy: {totalDocuments - totalMigratedDocuments:N0}");
            Console.WriteLine($"Successful batches: {successfulBatches}");
            Console.WriteLine($"Failed batches: {failedBatches}");

            if (totalTime.TotalSeconds > 0)
            {
                double overallSpeed = totalMigratedDocuments / totalTime.TotalSeconds;
                Console.WriteLine($"Overall speed: {overallSpeed:F2} documents/second");
                Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");
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
                    if (element.TryGetDouble(out double doubleValue))
                        return doubleValue;
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
