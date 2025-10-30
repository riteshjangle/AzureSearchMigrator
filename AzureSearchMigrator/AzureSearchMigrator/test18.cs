using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
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

                // Initialize clients
                var sourceClient = CreateSearchClient(sourceServiceName, sourceIndexName, sourceAdminApiKey);
                var targetClient = CreateSearchClient(targetServiceName, targetIndexName, targetAdminApiKey);

                // Step 1: Get actual document count
                Console.WriteLine("Step 1: Counting documents in source index...");
                int totalDocuments = await GetTotalDocumentCountAsync(sourceClient);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 2: Get index schema from source
                Console.WriteLine("Step 2: Retrieving index schema from source...");
                var indexSchema = await GetIndexSchemaAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                if (indexSchema == null)
                {
                    Console.WriteLine("Failed to retrieve index schema. Exiting.");
                    return;
                }

                // Step 3: Create index in target
                Console.WriteLine("Step 3: Creating index in target service...");
                await CreateTargetIndexAsync(targetServiceName, targetIndexName, targetAdminApiKey, indexSchema, apiVersion);

                // Step 4: Migrate all data using reliable pagination
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateAllDocumentsReliableAsync(sourceClient, targetClient, batchSize, totalDocuments);

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

        static SearchClient CreateSearchClient(string serviceName, string indexName, string apiKey)
        {
            Uri endpoint = new Uri($"https://{serviceName}.search.windows.net");
            AzureKeyCredential credential = new AzureKeyCredential(apiKey);
            return new SearchClient(endpoint, indexName, credential);
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

        static async Task<int> GetTotalDocumentCountAsync(SearchClient sourceClient)
        {
            try
            {
                var searchOptions = new SearchOptions
                {
                    IncludeTotalCount = true,
                    Size = 0
                };

                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                return (int)(results.TotalCount ?? 0);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count: {ex.Message}");
                return 0;
            }
        }

        static async Task MigrateAllDocumentsReliableAsync(
            SearchClient sourceClient,
            SearchClient targetClient,
            int batchSize,
            int totalDocuments)
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

                // First, let's get the key field name
                string keyFieldName = await GetKeyFieldNameAsync(sourceClient);
                Console.WriteLine($"Using key field for pagination: {keyFieldName}");

                // Use a cursor-based approach with the key field
                string lastKey = null;
                bool hasMoreDocuments = true;

                while (hasMoreDocuments && totalMigratedDocuments < totalDocuments)
                {
                    batchNumber++;

                    try
                    {
                        var options = new SearchOptions
                        {
                            Size = batchSize,
                            IncludeTotalCount = false,
                            OrderBy = { $"{keyFieldName} asc" },
                            Select = { "*" }
                        };

                        // If we have a last key, use it to get the next page
                        string searchQuery = "*";
                        if (!string.IsNullOrEmpty(lastKey))
                        {
                            searchQuery = $"{keyFieldName} gt '{EscapeSearchValue(lastKey)}'";
                        }

                        SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>(searchQuery, options);
                        
                        var batch = new List<SearchDocument>();
                        string currentLastKey = null;

                        await foreach (var result in results.GetResultsAsync())
                        {
                            var doc = result.Document;
                            batch.Add(doc);
                            
                            // Update the last key for pagination
                            if (doc.ContainsKey(keyFieldName))
                            {
                                currentLastKey = doc[keyFieldName]?.ToString();
                            }
                        }

                        if (batch.Count == 0)
                        {
                            Console.WriteLine("No more documents found.");
                            hasMoreDocuments = false;
                            break;
                        }

                        // Update lastKey for next iteration
                        lastKey = currentLastKey;

                        bool success = false;
                        int retryCount = 0;
                        const int maxRetries = 3;

                        while (!success && retryCount < maxRetries)
                        {
                            try
                            {
                                // Index the batch
                                IndexDocumentsResult indexResult = await targetClient.IndexDocumentsAsync(
                                    IndexDocumentsBatch.Upload(batch));

                                // Check if all documents succeeded
                                bool batchSuccess = true;
                                int failedInBatch = 0;
                                foreach (var result in indexResult.Results)
                                {
                                    if (!result.Succeeded)
                                    {
                                        batchSuccess = false;
                                        failedInBatch++;
                                    }
                                }

                                if (batchSuccess)
                                {
                                    successfulBatches++;
                                    totalMigratedDocuments += batch.Count;
                                    success = true;

                                    double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                                    var elapsed = DateTime.Now - startTime;
                                    var documentsPerSecond = totalMigratedDocuments / Math.Max(elapsed.TotalSeconds, 1);

                                    // Calculate ETA
                                    var remainingDocuments = totalDocuments - totalMigratedDocuments;
                                    var estimatedTimeRemaining = remainingDocuments / Math.Max(documentsPerSecond, 1);
                                    var eta = TimeSpan.FromSeconds(estimatedTimeRemaining);

                                    Console.WriteLine(
                                        $"✓ Batch {batchNumber}: {batch.Count,4} docs - " +
                                        $"Total: {totalMigratedDocuments,7:N0}/{totalDocuments,7:N0} ({percentage,5:F1}%) - " +
                                        $"Speed: {documentsPerSecond,6:F1} doc/s - " +
                                        $"ETA: {eta:hh\\:mm\\:ss}");
                                }
                                else
                                {
                                    retryCount++;
                                    if (retryCount < maxRetries)
                                    {
                                        Console.WriteLine($"⚠ Batch {batchNumber}: {failedInBatch} failed documents, retry {retryCount}/{maxRetries}");
                                        await Task.Delay(2000 * retryCount);
                                    }
                                    else
                                    {
                                        failedBatches++;
                                        int successfulInBatch = batch.Count - failedInBatch;
                                        totalMigratedDocuments += successfulInBatch;
                                        Console.WriteLine($"✗ Batch {batchNumber}: {failedInBatch} failed after {maxRetries} retries (Partial: {successfulInBatch}/{batch.Count})");
                                        success = true; // Continue despite failures
                                    }
                                }
                            }
                            catch (RequestFailedException ex) when (ex.Status == 429)
                            {
                                retryCount++;
                                if (retryCount < maxRetries)
                                {
                                    int delay = 5000 * retryCount;
                                    Console.WriteLine($"⚠ Batch {batchNumber}: Throttled, waiting {delay/1000}s...");
                                    await Task.Delay(delay);
                                }
                                else
                                {
                                    failedBatches++;
                                    Console.WriteLine($"✗ Batch {batchNumber}: Throttled after {maxRetries} retries");
                                    success = true; // Continue migration
                                }
                            }
                            catch (Exception ex)
                            {
                                retryCount++;
                                if (retryCount < maxRetries)
                                {
                                    Console.WriteLine($"⚠ Batch {batchNumber}: Error '{ex.Message}', retry {retryCount}/{maxRetries}");
                                    await Task.Delay(1000 * retryCount);
                                }
                                else
                                {
                                    failedBatches++;
                                    Console.WriteLine($"✗ Batch {batchNumber}: Failed after {maxRetries} retries: {ex.Message}");
                                    success = true; // Continue migration
                                }
                            }
                        }

                        // Check if we got fewer documents than requested (end of data)
                        if (batch.Count < batchSize)
                        {
                            hasMoreDocuments = false;
                        }

                        // Small delay to avoid overwhelming the service
                        if (batchNumber % 10 == 0)
                        {
                            await Task.Delay(500);
                        }

                        // Safety check - if we're not making progress, break
                        if (batchNumber > 500) // Safety limit
                        {
                            Console.WriteLine("Safety limit reached - stopping migration");
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"✗ Failed to search batch {batchNumber}: {ex.Message}");
                        failedBatches++;
                        // Try to continue with next batch
                        await Task.Delay(5000); // Wait before retrying
                    }
                }

                // Final summary
                var totalTime = DateTime.Now - startTime;
                Console.WriteLine(new string('=', 70));
                Console.WriteLine("=== MIGRATION COMPLETE ===");
                Console.WriteLine($"Total documents: {totalDocuments:N0}");
                Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Missing documents: {totalDocuments - totalMigratedDocuments:N0}");
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
                    Console.WriteLine("This might indicate an issue with the source index or pagination.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed at batch {batchNumber}: {ex.Message}");
                throw;
            }
        }

        static async Task<string> GetKeyFieldNameAsync(SearchClient client)
        {
            try
            {
                // Get index information to find the key field
                var options = new SearchOptions
                {
                    Size = 1,
                    Select = { "*" }
                };

                SearchResults<SearchDocument> results = await client.SearchAsync<SearchDocument>("*", options);
                
                await foreach (var result in results.GetResultsAsync())
                {
                    var doc = result.Document;
                    
                    // Common key field names in Azure Search
                    var possibleKeys = new[] { "id", "key", "documentId", "docId", "Id", "Key", "ID" };
                    
                    foreach (var key in possibleKeys)
                    {
                        if (doc.ContainsKey(key) && doc[key] != null)
                        {
                            Console.WriteLine($"Found key field: {key}");
                            return key;
                        }
                    }
                    
                    // If no common key found, try to find any field that looks like an ID
                    foreach (var key in doc.Keys)
                    {
                        if (key.ToLower().Contains("id") || key.ToLower().Contains("key"))
                        {
                            Console.WriteLine($"Using field as key: {key}");
                            return key;
                        }
                    }
                    
                    // Last resort: use the first field
                    var firstKey = doc.Keys.FirstOrDefault();
                    Console.WriteLine($"Using first field as key: {firstKey}");
                    return firstKey ?? "id";
                }
                
                return "id";
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not determine key field: {ex.Message}");
                return "id";
            }
        }

        static string EscapeSearchValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;
            
            // Escape special characters for Azure Search
            return value.Replace("'", "''")
                       .Replace("\\", "\\\\");
        }
    }
}
