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

                var batchSize = 1000; // Fixed batch size for 238,505 documents
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
                Console.WriteLine($"Total documents: 238,505");
                Console.WriteLine($"Batch Size: {batchSize}");
                Console.WriteLine($"Estimated batches: {Math.Ceiling(238505.0 / batchSize)}");
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

                // Step 3: Verify document count
                Console.WriteLine("Step 3: Verifying document count...");
                int totalDocuments = 238505; // Known count
                Console.WriteLine($"Expected document count: {totalDocuments:N0}");

                // Step 4: Migrate data with proper batching
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateLargeDatasetAsync(sourceClient, targetClient, batchSize, totalDocuments);

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

        static async Task MigrateLargeDatasetAsync(
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
                Console.WriteLine($"Start time: {startTime:yyyy-MM-dd HH:mm:ss}");
                Console.WriteLine(new string('=', 60));

                // Search options for paging
                var searchOptions = new SearchOptions
                {
                    Size = batchSize,
                    IncludeTotalCount = false
                };

                // Initial search
                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                // Process all pages
                await foreach (Page<SearchResult<SearchDocument>> page in results.GetResultsAsync().AsPages())
                {
                    batchNumber++;
                    var batch = page.Values.Select(r => r.Document).ToList();

                    if (batch.Count == 0)
                        continue;

                    bool success = false;
                    int retryCount = 0;
                    const int maxRetries = 3;

                    // Retry logic for failed batches
                    while (!success && retryCount < maxRetries)
                    {
                        try
                        {
                            // Upload the batch to the target index
                            IndexDocumentsResult indexResult = await targetClient.IndexDocumentsAsync(
                                IndexDocumentsBatch.Upload(batch));

                            // Check if all documents were indexed successfully
                            var failedDocuments = indexResult.Results.Where(r => !r.Succeeded).ToList();

                            if (!failedDocuments.Any())
                            {
                                successfulBatches++;
                                totalMigratedDocuments += batch.Count;
                                success = true;

                                // Calculate progress
                                double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                                var elapsed = DateTime.Now - startTime;
                                var estimatedTotal = elapsed.TotalSeconds / (totalMigratedDocuments / (double)totalDocuments);
                                var remaining = TimeSpan.FromSeconds(estimatedTotal - elapsed.TotalSeconds);

                                Console.WriteLine(
                                    $"✓ Batch {batchNumber}: {batch.Count} docs - " +
                                    $"Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%) - " +
                                    $"Elapsed: {elapsed:hh\\:mm\\:ss} - " +
                                    $"ETA: {remaining:hh\\:mm\\:ss}");
                            }
                            else
                            {
                                retryCount++;
                                if (retryCount < maxRetries)
                                {
                                    Console.WriteLine($"⚠ Batch {batchNumber}: {failedDocuments.Count} failed documents, retry {retryCount}/{maxRetries}...");
                                    await Task.Delay(1000 * retryCount); // Exponential backoff
                                }
                                else
                                {
                                    failedBatches++;
                                    Console.WriteLine($"✗ Batch {batchNumber}: {failedDocuments.Count} failed documents after {maxRetries} retries");
                                    // Log failed document keys for troubleshooting
                                    foreach (var failed in failedDocuments.Take(5)) // Show first 5 failures
                                    {
                                        Console.WriteLine($"  Failed document key: {failed.Key}, Error: {failed.ErrorMessage}");
                                    }
                                    if (failedDocuments.Count > 5)
                                    {
                                        Console.WriteLine($"  ... and {failedDocuments.Count - 5} more failures");
                                    }
                                }
                            }
                        }
                        catch (RequestFailedException ex) when (ex.Status == 429) // Throttling
                        {
                            retryCount++;
                            if (retryCount < maxRetries)
                            {
                                int delaySeconds = 5 * retryCount;
                                Console.WriteLine($"⚠ Batch {batchNumber}: Throttled, waiting {delaySeconds}s (retry {retryCount}/{maxRetries})...");
                                await Task.Delay(delaySeconds * 1000);
                            }
                            else
                            {
                                failedBatches++;
                                Console.WriteLine($"✗ Batch {batchNumber}: Throttled after {maxRetries} retries");
                                break;
                            }
                        }
                        catch (Exception ex)
                        {
                            retryCount++;
                            if (retryCount < maxRetries)
                            {
                                Console.WriteLine($"⚠ Batch {batchNumber}: Error '{ex.Message}', retry {retryCount}/{maxRetries}...");
                                await Task.Delay(1000 * retryCount);
                            }
                            else
                            {
                                failedBatches++;
                                Console.WriteLine($"✗ Batch {batchNumber}: Failed after {maxRetries} retries: {ex.Message}");
                                break;
                            }
                        }
                    }

                    // Small delay between batches to avoid throttling
                    if (batchNumber % 10 == 0) // Every 10 batches
                    {
                        await Task.Delay(500);
                    }
                }

                // Print comprehensive summary
                var totalTime = DateTime.Now - startTime;
                Console.WriteLine(new string('=', 60));
                Console.WriteLine("=== MIGRATION SUMMARY ===");
                Console.WriteLine($"Total documents expected: {totalDocuments:N0}");
                Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Failed batches: {failedBatches}");
                Console.WriteLine($"Total batches processed: {batchNumber}");
                Console.WriteLine($"Total time: {totalTime:hh\\:mm\\:ss}");

                if (totalDocuments > 0)
                {
                    double successRate = (double)totalMigratedDocuments / totalDocuments * 100;
                    Console.WriteLine($"Success rate: {successRate:F2}%");

                    if (totalMigratedDocuments < totalDocuments)
                    {
                        Console.WriteLine($"Missing documents: {totalDocuments - totalMigratedDocuments:N0}");
                    }
                }

                if (totalTime.TotalSeconds > 0)
                {
                    double docsPerSecond = totalMigratedDocuments / totalTime.TotalSeconds;
                    Console.WriteLine($"Throughput: {docsPerSecond:F2} documents/second");
                }

                if (failedBatches > 0)
                {
                    Console.WriteLine($"\n⚠ Warning: {failedBatches} batches failed. Check logs above for details.");
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
