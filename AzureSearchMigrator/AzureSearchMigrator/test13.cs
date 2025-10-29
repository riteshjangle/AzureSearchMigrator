using Azure;
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

                // Step 4: Migrate all data
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateAllDocumentsAsync(sourceClient, targetClient, batchSize, totalDocuments);

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

        static async Task MigrateAllDocumentsAsync(
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

                // Use search to get all documents with proper pagination
                var options = new SearchOptions
                {
                    Size = batchSize
                };

                // Execute search and get the results
                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", options);

                // Get the async enumerable for pages
                var resultsAsync = results.GetResultsAsync();

                // Process all pages using AsPages()
                await foreach (var page in resultsAsync.AsPages())
                {
                    batchNumber++;
                    var batch = new List<SearchDocument>();

                    foreach (var result in page.Values)
                    {
                        batch.Add(result.Document);
                    }

                    if (batch.Count == 0)
                        continue;

                    bool success = false;
                    int retryCount = 0;
                    const int maxRetries = 3;

                    while (!success && retryCount < maxRetries)
                    {
                        try
                        {
                            // Index the batch - the method might return IndexDocumentsResult directly
                            IndexDocumentsResult indexResult = await targetClient.IndexDocumentsAsync(
                                IndexDocumentsBatch.Upload(batch));

                            // Check if all documents succeeded
                            bool batchSuccess = true;
                            foreach (var result in indexResult.Results)
                            {
                                if (!result.Succeeded)
                                {
                                    batchSuccess = false;
                                    break;
                                }
                            }

                            if (batchSuccess)
                            {
                                successfulBatches++;
                                totalMigratedDocuments += batch.Count;
                                success = true;

                                double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                                var elapsed = DateTime.Now - startTime;
                                var documentsPerSecond = totalMigratedDocuments / elapsed.TotalSeconds;

                                // Calculate ETA
                                var remainingDocuments = totalDocuments - totalMigratedDocuments;
                                var estimatedTimeRemaining = remainingDocuments / documentsPerSecond;
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
                                    Console.WriteLine($"⚠ Batch {batchNumber}: Some documents failed, retry {retryCount}/{maxRetries}");
                                    await Task.Delay(2000 * retryCount);
                                }
                                else
                                {
                                    failedBatches++;
                                    Console.WriteLine($"✗ Batch {batchNumber}: Some documents failed after {maxRetries} retries");
                                    // Count successful documents in this batch
                                    int successfulInBatch = indexResult.Results.Count(r => r.Succeeded);
                                    totalMigratedDocuments += successfulInBatch;
                                    Console.WriteLine($"  Partial success: {successfulInBatch}/{batch.Count} documents indexed");
                                    success = true; // Mark as "handled" to continue
                                }
                            }
                        }
                        catch (RequestFailedException ex) when (ex.Status == 429)
                        {
                            retryCount++;
                            if (retryCount < maxRetries)
                            {
                                int delay = 5000 * retryCount;
                                Console.WriteLine($"⚠ Batch {batchNumber}: Throttled, waiting {delay / 1000}s...");
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

                    // Small delay to avoid overwhelming the service
                    if (batchNumber % 10 == 0)
                    {
                        await Task.Delay(300);
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

                if (failedBatches > 0)
                {
                    Console.WriteLine($"\nNote: {failedBatches} batches had failures. Check logs for details.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed at batch {batchNumber}: {ex.Message}");
                throw;
            }
        }
    }
}
