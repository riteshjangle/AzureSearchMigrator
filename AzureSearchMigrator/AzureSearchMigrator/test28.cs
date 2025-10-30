using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using static Azure.Search.Documents.Indexes.Models.LexicalAnalyzerName;
using static System.Runtime.InteropServices.JavaScript.JSType;

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

                var batchSize = 1000;
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
                    Console.WriteLine($"Failed to get document count via API. Status: {response.StatusCode}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count via API: {ex.Message}");
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
                                        var keyFieldName = field.GetProperty("name").GetString();
                                        Console.WriteLine($"Found key field in schema: {keyFieldName}");
                                        return keyFieldName;
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

            // Fallback: try to get from a sample document
            try
            {
                using (var httpClient = new HttpClient())
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
                                // Common key field names
                                var possibleKeys = new[] { "id", "key", "documentId", "docId", "Id", "Key", "ID", "identifier" };
                                foreach (var prop in firstDoc.EnumerateObject())
                                {
                                    if (possibleKeys.Contains(prop.Name.ToLower()))
                                    {
                                        Console.WriteLine($"Found key field from document sample: {prop.Name}");
                                        return prop.Name;
                                    }
                                }
                                // Return first field as fallback
                                var fallbackKey = firstDoc.EnumerateObject().First().Name;
                                Console.WriteLine($"Using fallback key field: {fallbackKey}");
                                return fallbackKey;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not determine key field from document: {ex.Message}");
            }
            
            Console.WriteLine("Using default key field name: 'id'");
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

                // Strategy 1: Try cursor-based pagination first
                Console.WriteLine("Attempting cursor-based pagination...");
                var cursorResult = await MigrateWithCursorPagination(
                    sourceServiceName, sourceIndexName, sourceApiKey,
                    targetServiceName, targetIndexName, targetApiKey,
                    batchSize, totalDocuments, keyFieldName, apiVersion);

                totalMigratedDocuments = cursorResult.migratedCount;
                successfulBatches = cursorResult.successfulBatches;
                failedBatches = cursorResult.failedBatches;
                batchNumber = cursorResult.totalBatches;

                // Strategy 2: If cursor pagination didn't get all documents, try skip-based pagination
                if (totalMigratedDocuments < totalDocuments)
                {
                    int remainingDocuments = totalDocuments - totalMigratedDocuments;
                    Console.WriteLine($"\nCursor pagination missed {remainingDocuments:N0} documents. Trying skip-based pagination...");
                    
                    var skipResult = await MigrateWithSkipPagination(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        targetServiceName, targetIndexName, targetApiKey,
                        batchSize, remainingDocuments, totalMigratedDocuments, keyFieldName, apiVersion);

                    totalMigratedDocuments += skipResult.migratedCount;
                    successfulBatches += skipResult.successfulBatches;
                    failedBatches += skipResult.failedBatches;
                    batchNumber += skipResult.totalBatches;
                }

                // Strategy 3: If still missing documents, try full scan with different ordering
                if (totalMigratedDocuments < totalDocuments)
                {
                    int stillMissing = totalDocuments - totalMigratedDocuments;
                    Console.WriteLine($"\nStill missing {stillMissing:N0} documents. Trying full scan...");
                    
                    var fullScanResult = await MigrateWithFullScan(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        targetServiceName, targetIndexName, targetApiKey,
                        batchSize, keyFieldName, apiVersion);

                    totalMigratedDocuments = fullScanResult.migratedCount;
                    successfulBatches += fullScanResult.successfulBatches;
                    failedBatches += fullScanResult.failedBatches;
                    batchNumber += fullScanResult.totalBatches;
                }

                // Final summary
                var totalTime = DateTime.Now - startTime;
                Console.WriteLine(new string('=', 70));
                Console.WriteLine("=== MIGRATION COMPLETE ===");
                Console.WriteLine($"Total documents: {totalDocuments:N0}");
                Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Missing documents: {Math.Max(0, totalDocuments - totalMigratedDocuments):N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Failed batches: {failedBatches}");
                Console.WriteLine($"Total batches processed: {batchNumber}");
                Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");

                if (totalTime.TotalSeconds > 0)
                {
                    double overallSpeed = totalMigratedDocuments / totalTime.TotalSeconds;
                    Console.WriteLine($"Overall speed: {overallSpeed:F2} documents/second");
                }

                if (totalDocuments > 0)
                {
                    double successRate = (double)totalMigratedDocuments / totalDocuments * 100;
                    Console.WriteLine($"Success rate: {successRate:F2}%");
                }

                if (totalMigratedDocuments < totalDocuments)
                {
                    Console.WriteLine($"\n⚠ Warning: Only {totalMigratedDocuments:N0} out of {totalDocuments:N0} documents were migrated.");
                    Console.WriteLine("This could be due to:");
                    Console.WriteLine("  - Documents being deleted during migration");
                    Console.WriteLine("  - Inconsistent document counts");
                    Console.WriteLine("  - API limitations");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed: {ex.Message}");
                throw;
            }
        }

        static async Task<(int migratedCount, int successfulBatches, int failedBatches, int totalBatches)> MigrateWithCursorPagination(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string keyFieldName, string apiVersion)
        {
            int migratedCount = 0;
            int successfulBatches = 0;
            int failedBatches = 0;
            int batchNumber = 0;

            string lastKey = null;
            bool hasMoreDocuments = true;
            var processedKeys = new HashSet<string>();

            while (hasMoreDocuments && migratedCount < totalDocuments)
            {
                batchNumber++;

                try
                {
                    var documents = await GetDocumentsWithCursorAsync(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        batchSize, keyFieldName, lastKey, apiVersion);

                    if (documents == null || documents.Count == 0)
                    {
                        Console.WriteLine($"Batch {batchNumber}: No documents returned - stopping cursor pagination");
                        hasMoreDocuments = false;
                        break;
                    }

                    // Filter out already processed documents
                    var newDocuments = new List<Dictionary<string, object>>();
                    foreach (var doc in documents)
                    {
                        if (doc.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                        {
                            var key = keyValue.ToString();
                            if (!processedKeys.Contains(key))
                            {
                                processedKeys.Add(key);
                                newDocuments.Add(doc);
                            }
                        }
                    }

                    if (newDocuments.Count == 0)
                    {
                        Console.WriteLine($"Batch {batchNumber}: All documents already processed - stopping cursor pagination");
                        hasMoreDocuments = false;
                        break;
                    }

                    // Update the last key for next pagination
                    if (newDocuments.Count > 0)
                    {
                        var lastDocument = newDocuments.Last();
                        if (lastDocument.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                        {
                            lastKey = keyValue.ToString();
                        }
                    }

                    // Index the batch
                    bool batchSuccess = await IndexDocumentsViaApiAsync(
                        targetServiceName, targetIndexName, targetApiKey,
                        newDocuments, apiVersion);

                    if (batchSuccess)
                    {
                        successfulBatches++;
                        migratedCount += newDocuments.Count;

                        double percentage = (double)migratedCount / totalDocuments * 100;
                        Console.WriteLine(
                            $"✓ Cursor Batch {batchNumber}: {newDocuments.Count,4} docs - " +
                            $"Total: {migratedCount,7:N0}/{totalDocuments,7:N0} ({percentage,5:F1}%)");
                    }
                    else
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Cursor Batch {batchNumber}: Failed to index documents");
                    }

                    // Check if we got fewer documents than requested (end of data)
                    if (documents.Count < batchSize)
                    {
                        Console.WriteLine($"Got fewer documents ({documents.Count}) than batch size ({batchSize}) - end of cursor data");
                        hasMoreDocuments = false;
                    }

                    // Small delay to avoid overwhelming the service
                    if (batchNumber % 10 == 0)
                    {
                        await Task.Delay(1000);
                    }
                }
                catch (Exception ex)
                {
                    failedBatches++;
                    Console.WriteLine($"✗ Failed to process cursor batch {batchNumber}: {ex.Message}");
                    await Task.Delay(5000);
                }

                // Safety check
                if (batchNumber > 500)
                {
                    Console.WriteLine("Cursor pagination safety limit reached");
                    break;
                }
            }

            return (migratedCount, successfulBatches, failedBatches, batchNumber);
        }

        static async Task<(int migratedCount, int successfulBatches, int failedBatches, int totalBatches)> MigrateWithSkipPagination(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int remainingDocuments, int alreadyMigrated, string keyFieldName, string apiVersion)
        {
            int migratedCount = 0;
            int successfulBatches = 0;
            int failedBatches = 0;
            int batchNumber = 0;

            int skip = alreadyMigrated;
            var processedKeys = new HashSet<string>();

            while (migratedCount < remainingDocuments)
            {
                batchNumber++;

                try
                {
                    var documents = await GetDocumentsWithSkipAsync(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        batchSize, skip, apiVersion);

                    if (documents == null || documents.Count == 0)
                    {
                        Console.WriteLine($"Skip Batch {batchNumber}: No documents returned - stopping skip pagination");
                        break;
                    }

                    // Filter out already processed documents
                    var newDocuments = new List<Dictionary<string, object>>();
                    foreach (var doc in documents)
                    {
                        if (doc.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                        {
                            var key = keyValue.ToString();
                            if (!processedKeys.Contains(key))
                            {
                                processedKeys.Add(key);
                                newDocuments.Add(doc);
                            }
                        }
                    }

                    if (newDocuments.Count == 0)
                    {
                        Console.WriteLine($"Skip Batch {batchNumber}: All documents already processed");
                        skip += batchSize;
                        continue;
                    }

                    // Index the batch
                    bool batchSuccess = await IndexDocumentsViaApiAsync(
                        targetServiceName, targetIndexName, targetApiKey,
                        newDocuments, apiVersion);

                    if (batchSuccess)
                    {
                        successfulBatches++;
                        migratedCount += newDocuments.Count;
                        skip += batchSize;

                        Console.WriteLine(
                            $"✓ Skip Batch {batchNumber}: {newDocuments.Count,4} docs - " +
                            $"Total migrated via skip: {migratedCount,7:N0}");
                    }
                    else
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Skip Batch {batchNumber}: Failed to index documents");
                    }

                    // Small delay to avoid overwhelming the service
                    if (batchNumber % 5 == 0)
                    {
                        await Task.Delay(2000);
                    }
                }
                catch (Exception ex)
                {
                    failedBatches++;
                    Console.WriteLine($"✗ Failed to process skip batch {batchNumber}: {ex.Message}");
                    await Task.Delay(5000);
                }

                // Safety check
                if (batchNumber > 100)
                {
                    Console.WriteLine("Skip pagination safety limit reached");
                    break;
                }
            }

            return (migratedCount, successfulBatches, failedBatches, batchNumber);
        }

        static async Task<(int migratedCount, int successfulBatches, int failedBatches, int totalBatches)> MigrateWithFullScan(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, string keyFieldName, string apiVersion)
        {
            int migratedCount = 0;
            int successfulBatches = 0;
            int failedBatches = 0;
            int batchNumber = 0;

            // Try different orderings to catch all documents
            var orderings = new[] { "asc", "desc" };

            foreach (var ordering in orderings)
            {
                Console.WriteLine($"Full scan with {ordering} ordering...");
                string lastKey = null;
                bool hasMore = true;
                var processedKeys = new HashSet<string>();

                while (hasMore)
                {
                    batchNumber++;

                    try
                    {
                        var documents = await GetDocumentsWithOrderingAsync(
                            sourceServiceName, sourceIndexName, sourceApiKey,
                            batchSize, keyFieldName, lastKey, ordering, apiVersion);

                        if (documents == null || documents.Count == 0)
                        {
                            hasMore = false;
                            break;
                        }

                        // Filter and process documents
                        var newDocuments = new List<Dictionary<string, object>>();
                        foreach (var doc in documents)
                        {
                            if (doc.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                            {
                                var key = keyValue.ToString();
                                if (!processedKeys.Contains(key))
                                {
                                    processedKeys.Add(key);
                                    newDocuments.Add(doc);
                                }
                            }
                        }

                        if (newDocuments.Count > 0)
                        {
                            // Update last key
                            var lastDocument = newDocuments.Last();
                            if (lastDocument.TryGetValue(keyFieldName, out var keyValue) && keyValue != null)
                            {
                                lastKey = keyValue.ToString();
                            }

                            // Index batch
                            bool success = await IndexDocumentsViaApiAsync(
                                targetServiceName, targetIndexName, targetApiKey,
                                newDocuments, apiVersion);

                            if (success)
                            {
                                successfulBatches++;
                                migratedCount += newDocuments.Count;
                                Console.WriteLine($"✓ FullScan {ordering} Batch {batchNumber}: {newDocuments.Count} docs - Total: {migratedCount:N0}");
                            }
                            else
                            {
                                failedBatches++;
                            }
                        }

                        if (documents.Count < batchSize)
                        {
                            hasMore = false;
                        }

                        await Task.Delay(500);
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ FullScan batch failed: {ex.Message}");
                        await Task.Delay(2000);
                    }
                }
            }

            return (migratedCount, successfulBatches, failedBatches, batchNumber);
        }

        static async Task<List<Dictionary<string, object>>> GetDocumentsWithCursorAsync(
            string serviceName, string indexName, string apiKey,
            int top, string keyFieldName, string lastKey, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Build the filter for cursor-based pagination
                    string filter = null;
                    if (!string.IsNullOrEmpty(lastKey))
                    {
                        filter = $"{keyFieldName} gt '{EscapeODataString(lastKey)}'";
                    }

                    var urlBuilder = new StringBuilder();
                    urlBuilder.Append($"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?");
                    urlBuilder.Append($"api-version={apiVersion}");
                    urlBuilder.Append($"&$top={top}");
                    urlBuilder.Append($"&$orderby={Uri.EscapeDataString(keyFieldName)} asc");
                    urlBuilder.Append("&search=*");

                    if (!string.IsNullOrEmpty(filter))
                    {
                        urlBuilder.Append($"&$filter={Uri.EscapeDataString(filter)}");
                    }

                    var url = urlBuilder.ToString();
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        return ParseDocumentsFromJson(json, keyFieldName);
                    }
                    else
                    {
                        var errorContent = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to get documents: {response.StatusCode}");
                        return new List<Dictionary<string, object>>();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting documents: {ex.Message}");
                return new List<Dictionary<string, object>>();
            }
        }

        static async Task<List<Dictionary<string, object>>> GetDocumentsWithSkipAsync(
            string serviceName, string indexName, string apiKey,
            int top, int skip, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?" +
                              $"api-version={apiVersion}&$top={top}&$skip={skip}&search=*";

                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);
                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        return ParseDocumentsFromJson(json, "id"); // key field not needed for parsing
                    }
                    else
                    {
                        Console.WriteLine($"Failed to get skip documents: {response.StatusCode}");
                        return new List<Dictionary<string, object>>();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting skip documents: {ex.Message}");
                return new List<Dictionary<string, object>>();
            }
        }

        static async Task<List<Dictionary<string, object>>> GetDocumentsWithOrderingAsync(
            string serviceName, string indexName, string apiKey,
            int top, string keyFieldName, string lastKey, string ordering, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    string filter = null;
                    if (!string.IsNullOrEmpty(lastKey))
                    {
                        if (ordering == "asc")
                            filter = $"{keyFieldName} gt '{EscapeODataString(lastKey)}'";
                        else
                            filter = $"{keyFieldName} lt '{EscapeODataString(lastKey)}'";
                    }

                    var urlBuilder = new StringBuilder();
                    urlBuilder.Append($"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?");
                    urlBuilder.Append($"api-version={apiVersion}");
                    urlBuilder.Append($"&$top={top}");
                    urlBuilder.Append($"&$orderby={Uri.EscapeDataString(keyFieldName)} {ordering}");
                    urlBuilder.Append("&search=*");

                    if (!string.IsNullOrEmpty(filter))
                    {
                        urlBuilder.Append($"&$filter={Uri.EscapeDataString(filter)}");
                    }

                    var url = urlBuilder.ToString();
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var response = await httpClient.GetAsync(url);

                    if (response.IsSuccessStatusCode)
                    {
                        var json = await response.Content.ReadAsStringAsync();
                        return ParseDocumentsFromJson(json, keyFieldName);
                    }
                    else
                    {
                        return new List<Dictionary<string, object>>();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting ordered documents: {ex.Message}");
                return new List<Dictionary<string, object>>();
            }
        }

        static List<Dictionary<string, object>> ParseDocumentsFromJson(string json, string keyFieldName)
        {
            var documents = new List<Dictionary<string, object>>();

            try
            {
                using (var doc = JsonDocument.Parse(json))
                {
                    if (doc.RootElement.TryGetProperty("value", out var valueArray))
                    {
                        foreach (var element in valueArray.EnumerateArray())
                        {
                            var documentDict = new Dictionary<string, object>();
                            foreach (var prop in element.EnumerateObject())
                            {
                                documentDict[prop.Name] = GetValueFromJsonElement(prop.Value);
                            }
                            documents.Add(documentDict);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error parsing documents JSON: {ex.Message}");
            }

            return documents;
        }

        static async Task<bool> IndexDocumentsViaApiAsync(
            string serviceName, string indexName, string apiKey,
            List<Dictionary<string, object>> documents, string apiVersion)
        {
            if (documents == null || documents.Count == 0)
                return true;

            try
            {
                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/index?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    // Create the correct indexing request format
                    var indexRequestValues = new List<object>();
                    foreach (var document in documents)
                    {
                        var combinedDocument = new Dictionary<string, object>();
                        combinedDocument["@search.action"] = "upload";
                        foreach (var field in document)
                        {
                            combinedDocument[field.Key] = field.Value;
                        }
                        indexRequestValues.Add(combinedDocument);
                    }

                    var indexRequest = new { value = indexRequestValues };

                    var jsonOptions = new JsonSerializerOptions
                    {
                        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
                    };

                    var jsonContent = JsonSerializer.Serialize(indexRequest, jsonOptions);
                    var content = new StringContent(jsonContent, Encoding.UTF8, "application/json");
                    
                    var response = await httpClient.PostAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        return true;
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        Console.WriteLine($"Failed to index batch: {response.StatusCode}");
                        return false;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error indexing documents: {ex.Message}");
                return false;
            }
        }

        static string EscapeODataString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            return value.Replace("'", "''")
                       .Replace("\\", "\\\\")
                       .Replace("\"", "\\\"")
                       .Replace("\n", "\\n")
                       .Replace("\r", "\\r")
                       .Replace("\t", "\\t");
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
                    return element.GetRawText();
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
