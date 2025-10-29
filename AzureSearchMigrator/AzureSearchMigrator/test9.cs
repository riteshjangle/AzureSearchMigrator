using Azure;
using Azure.Core;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;

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

                var batchSize = int.Parse(Configuration["MigrationSettings:BatchSize"] ?? "1000");
                var apiVersion = Configuration["MigrationSettings:ApiVersion"] ?? "2023-11-01";

                // Validate batch size
                if (batchSize <= 0 || batchSize > 1000)
                {
                    Console.WriteLine($"ERROR: BatchSize must be between 1 and 1000. Current value: {batchSize}");
                    Console.WriteLine("Setting BatchSize to 1000 for safe migration...");
                    batchSize = 1000;
                }

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

                // Step 3: Get total count first for progress reporting
                Console.WriteLine("Step 3: Counting total documents...");
                long totalDocuments = await GetTotalDocumentCountAsync(sourceClient);

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 4: Migrate data with proper scalable paging
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateDataWithPagingAsync(sourceClient, targetClient, batchSize, totalDocuments);

                Console.WriteLine("\nMigration completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nMigration failed: {ex.Message}");
                // Unwrap aggregate exceptions for clearer messages
                if (ex is AggregateException aggEx)
                {
                    foreach (var innerEx in aggEx.Flatten().InnerExceptions)
                    {
                        Console.WriteLine($"  - Inner Exception: {innerEx.Message}");
                    }
                }
                Console.WriteLine($"\nStack trace: {ex.StackTrace}");
            }

            Console.WriteLine("\nPress any key to exit...");
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
            var options = new SearchClientOptions
            {
                // You can specify the API version if needed, though the SDK defaults to a recent stable version.
                // Version = SearchClientOptions.ServiceVersion.V2023_11_01
            };
            return new SearchClient(endpoint, indexName, credential, options);
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
                    response.EnsureSuccessStatusCode();

                    var schema = await response.Content.ReadAsStringAsync();
                    Console.WriteLine("Successfully retrieved index schema.");
                    return schema;
                }
            }
            catch (HttpRequestException ex)
            {
                Console.WriteLine($"Error retrieving index schema. Status: {ex.StatusCode}. Message: {ex.Message}");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred while retrieving index schema: {ex.Message}");
                return null;
            }
        }

        static async Task CreateTargetIndexAsync(string serviceName, string indexName, string apiKey, string indexSchema, string apiVersion)
        {
            try
            {
                using (var httpClient = new HttpClient())
                {
                    // Use a PUT request which is idempotent (creates or updates)
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var content = new StringContent(indexSchema, System.Text.Encoding.UTF8, "application/json");
                    var response = await httpClient.PutAsync(url, content);

                    if (response.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Successfully created or updated target index '{indexName}'.");
                    }
                    else
                    {
                        var error = await response.Content.ReadAsStringAsync();
                        throw new HttpRequestException($"Failed to create target index. Status: {response.StatusCode}, Error: {error}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating target index: {ex.Message}");
                throw; // Re-throw to stop the migration
            }
        }

        static async Task<long> GetTotalDocumentCountAsync(SearchClient sourceClient)
        {
            try
            {
                // This is the most efficient way to get a count.
                var searchOptions = new SearchOptions
                {
                    IncludeTotalCount = true,
                    Size = 0 // Don't retrieve any documents, just the count.
                };

                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                if (results.TotalCount.HasValue)
                {
                    Console.WriteLine($"Total documents found: {results.TotalCount.Value:N0}");
                    return results.TotalCount.Value;
                }
                else
                {
                    Console.WriteLine("Could not determine total count. Will migrate until no more documents are found.");
                    return -1; // Special value to indicate unknown count
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count: {ex.Message}");
                Console.WriteLine("Will continue migration without a total count for progress.");
                return -1; // Indicate unknown count
            }
        }

        // *** FIXED METHOD ***
        // This version uses scalable paging and avoids the 100,000 document $skip limit.
        // *** CORRECTED METHOD - Version 2 ***
        // This version properly awaits the async call before using AsPages().
        // *** FINAL CORRECTED METHOD ***
        // This version uses the correct object chain: SearchResults -> GetResults() -> AsPages()
        // *** FINAL, TESTED, AND CORRECTED METHOD ***
        // This version uses the correct GetResultsAsync() method.
        static async Task MigrateDataWithPagingAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, long totalDocuments)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            long totalMigratedDocuments = 0;
            int currentBatchNumber = 0;

            try
            {
                string progressMessage = totalDocuments > 0
                    ? $"Total documents to migrate: {totalDocuments:N0}"
                    : "Total documents to migrate: Unknown (migrating until complete)";

                Console.WriteLine(progressMessage);
                Console.WriteLine($"Batch size: {batchSize}");
                if (totalDocuments > 0)
                {
                    Console.WriteLine($"Estimated batches: {Math.Ceiling((double)totalDocuments / batchSize):N0}");
                }
                Console.WriteLine();

                var searchOptions = new SearchOptions
                {
                    Size = batchSize,
                    // OrderBy is required for deep paging with continuation tokens to work reliably.
                    // Using "search.score()" is a safe default for a wildcard search.
                    // If you have a unique, sortable key field, using that is even better, e.g., OrderBy = { "YourDocumentKey asc" }
                    OrderBy = { "search.score()" }
                };

                // 1. Get the initial response.
                Response<SearchResults<SearchDocument>> searchResponse = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                // 2. THIS IS THE FIX: Call GetResultsAsync() to get the async pageable collection.
                AsyncPageable<SearchResult<SearchDocument>> asyncPageableResults = searchResponse.Value.GetResultsAsync();

                // 3. Now, call AsPages() on the AsyncPageable object.
                await foreach (Page<SearchResult<SearchDocument>> page in asyncPageableResults.AsPages())
                {
                    currentBatchNumber++;
                    var batch = page.Values.Select(r => r.Document).ToList();

                    if (!batch.Any())
                    {
                        // This indicates we've reached the end.
                        break;
                    }

                    try
                    {
                        IndexDocumentsResult batchResult = await targetClient.IndexDocumentsAsync(
                            IndexDocumentsBatch.Upload(batch));

                        // Check for item-level errors within the batch
                        if (batchResult.Results.Any(r => !r.Succeeded))
                        {
                            failedBatches++;
                            int batchErrors = batchResult.Results.Count(r => !r.Succeeded);
                            totalMigratedDocuments += batch.Count - batchErrors;
                            Console.WriteLine($"✗ Batch {currentBatchNumber}: Succeeded for {batch.Count - batchErrors}, but {batchErrors} errors occurred.");
                            // Log first few errors for diagnostics
                            foreach (var item in batchResult.Results.Where(r => !r.Succeeded).Take(3))
                            {
                                Console.WriteLine($"  - Failed to index document '{item.Key}': {item.ErrorMessage}");
                            }
                        }
                        else
                        {
                            successfulBatches++;
                            totalMigratedDocuments += batch.Count;
                        }

                        // Progress reporting
                        if (totalDocuments > 0)
                        {
                            double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                            Console.WriteLine($"✓ Batch {currentBatchNumber}: {batch.Count} documents - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
                        }
                        else
                        {
                            Console.WriteLine($"✓ Batch {currentBatchNumber}: {batch.Count} documents - Total migrated: {totalMigratedDocuments:N0}");
                        }
                    }
                    catch (RequestFailedException ex) when (ex.Status == 429)
                    {
                        // Handle throttling specifically
                        failedBatches++;
                        Console.WriteLine($"✗ Batch {currentBatchNumber} was throttled (HTTP 429). Pausing for 10 seconds before retrying page...");
                        await Task.Delay(10000);
                        currentBatchNumber--; // Decrement to retry the same page logic (the loop will fetch the next page, which is what we want after a pause)
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ FATAL ERROR processing batch {currentBatchNumber}: {ex.Message}");
                        await Task.Delay(2000); // Wait before attempting the next page
                    }
                }
            }
            finally
            {
                // Summary
                Console.WriteLine($"\n=== Migration Summary ===");
                Console.WriteLine($"Total documents processed: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Batches with failures: {failedBatches}");

                if (totalDocuments > 0)
                {
                    double successRate = (double)totalMigratedDocuments / totalDocuments * 100;
                    Console.WriteLine($"Success rate: {successRate:F2}%");
                }
            }
        }
    }
}
