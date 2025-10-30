using Azure;
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
                int totalDocuments = await GetTotalDocumentCountAsync(sourceClient);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 4: Migrate all data using reliable approach
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

                // Use multiple search queries with different filters to ensure we get all documents
                // This approach avoids skip limits and cursor issues

                // First, let's try to get all documents using the SDK with proper async enumeration
                var searchOptions = new SearchOptions
                {
                    Size = batchSize
                };

                // Execute search and process all pages
                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                
                var resultsAsync = results.GetResultsAsync();
                await foreach (var page in resultsAsync.AsPages())
                {
                    batchNumber++;
                    var batch = new List<SearchDocument>();

                    foreach (var result in page.Values)
                    {
                        batch.Add(result.Document);
                    }

                    if (batch.Count == 0)
                    {
                        Console.WriteLine("No documents in current page, continuing...");
                        continue;
                    }

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

                    // Small delay to avoid overwhelming the service
                    if (batchNumber % 10 == 0)
                    {
                        await Task.Delay(500);
                    }
                }

                // If we still have missing documents, try alternative approaches
                if (totalMigratedDocuments < totalDocuments)
                {
                    Console.WriteLine($"\nFirst pass completed with {totalMigratedDocuments:N0}/{totalDocuments:N0} documents.");
                    Console.WriteLine("Starting second pass to capture missing documents...");

                    await MigrateMissingDocumentsAsync(sourceClient, targetClient, totalDocuments, totalMigratedDocuments);
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
                    Console.WriteLine($"\n⚠ Warning: {totalDocuments - totalMigratedDocuments:N0} documents could not be migrated.");
                    Console.WriteLine("This might be due to index inconsistencies or document access issues.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed at batch {batchNumber}: {ex.Message}");
                throw;
            }
        }

        static async Task MigrateMissingDocumentsAsync(
            SearchClient sourceClient,
            SearchClient targetClient,
            int totalDocuments,
            int alreadyMigrated)
        {
            int additionalBatches = 0;
            int additionalDocuments = 0;

            try
            {
                Console.WriteLine("Attempting to find missing documents using different search strategies...");

                // Strategy 1: Try with different page sizes
                var strategies = new[] { 500, 250, 100 };
                foreach (var pageSize in strategies)
                {
                    if (alreadyMigrated + additionalDocuments >= totalDocuments)
                        break;

                    Console.WriteLine($"Trying with page size: {pageSize}");
                    
                    var searchOptions = new SearchOptions
                    {
                        Size = pageSize
                    };

                    SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                    var resultsAsync = results.GetResultsAsync();

                    await foreach (var page in resultsAsync.AsPages())
                    {
                        var batch = new List<SearchDocument>();
                        foreach (var result in page.Values)
                        {
                            batch.Add(result.Document);
                        }

                        if (batch.Count == 0) break;

                        try
                        {
                            var indexResult = await targetClient.IndexDocumentsAsync(IndexDocumentsBatch.Upload(batch));
                            int successfulInBatch = batch.Count - indexResult.Results.Count(r => !r.Succeeded);
                            additionalDocuments += successfulInBatch;
                            additionalBatches++;

                            Console.WriteLine($"Additional batch {additionalBatches}: {successfulInBatch} docs (Page size: {pageSize})");

                            if (alreadyMigrated + additionalDocuments >= totalDocuments)
                                break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error in additional batch: {ex.Message}");
                        }

                        await Task.Delay(300);
                    }
                }

                Console.WriteLine($"Additional migration: {additionalDocuments} documents in {additionalBatches} batches");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error in missing documents migration: {ex.Message}");
            }
        }
    }
}
