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

                var batchSize = int.Parse(Configuration["MigrationSettings:BatchSize"] ?? "500");
                var apiVersion = Configuration["MigrationSettings:ApiVersion"] ?? "2023-11-01";

                // Validate batch size
                if (batchSize <= 0 || batchSize > 1000)
                {
                    Console.WriteLine($"ERROR: BatchSize must be between 1 and 1000. Current value: {batchSize}");
                    Console.WriteLine("Setting BatchSize to 500 for safe migration...");
                    batchSize = 500;
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

                // Step 3: Get total count first
                Console.WriteLine("Step 3: Counting total documents...");
                int totalDocuments = await GetTotalDocumentCountAsync(sourceClient);

                if (totalDocuments == 0)
                {
                    Console.WriteLine("No documents found to migrate.");
                    return;
                }

                // Step 4: Migrate data using search continuation (no skip limit)
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateDataWithContinuationAsync(sourceClient, targetClient, batchSize, totalDocuments);

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
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error creating target index: {ex.Message}");
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

                await foreach (var result in results.GetResultsAsync())
                {
                    // This loop won't execute because Size = 0
                }

                long totalCount = results.TotalCount ?? 0;
                Console.WriteLine($"Total documents found: {totalCount:N0}");
                return (int)totalCount;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count: {ex.Message}");
                return -1;
            }
        }

        static async Task MigrateDataWithContinuationAsync(
    SearchClient sourceClient,
    SearchClient targetClient,
    int batchSize,
    int totalDocuments)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            int batchNumber = 0;

            try
            {
                Console.WriteLine($"Total documents to migrate: {totalDocuments:N0}");
                Console.WriteLine($"Batch size: {batchSize}");
                Console.WriteLine($"Estimated batches: {Math.Ceiling((double)totalDocuments / batchSize):N0}");
                Console.WriteLine();

                var searchOptions = new SearchOptions
                {
                    Size = batchSize,
                    IncludeTotalCount = false
                };

                // Start search
                var searchResponse = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                // Use the new pagination API
                await foreach (Page<SearchResult<SearchDocument>> page in searchResponse.Value.GetResultsAsync().AsPages())
                {
                    batchNumber++;
                    var batch = new List<SearchDocument>();

                    try
                    {
                        foreach (var result in page.Values)
                        {
                            batch.Add(result.Document);
                        }

                        if (batch.Count == 0)
                        {
                            Console.WriteLine("No more documents found — migration complete.");
                            break;
                        }

                        // Upload the batch to the target index
                        IndexDocumentsResult batchResult = await targetClient.IndexDocumentsAsync(
                            IndexDocumentsBatch.Upload(batch));

                        // Check for errors
                        bool batchSuccess = true;
                        int batchErrors = 0;

                        foreach (var item in batchResult.Results)
                        {
                            if (!item.Succeeded)
                            {
                                batchSuccess = false;
                                batchErrors++;
                                if (batchErrors <= 3)
                                {
                                    Console.WriteLine($"  Failed to index document {item.Key}: {item.ErrorMessage}");
                                }
                            }
                        }

                        if (batchSuccess)
                        {
                            successfulBatches++;
                            totalMigratedDocuments += batch.Count;
                            double percentage = totalDocuments > 0
                                ? (double)totalMigratedDocuments / totalDocuments * 100
                                : 0;
                            Console.WriteLine($"✓ Batch {batchNumber}: {batch.Count} docs - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
                        }
                        else
                        {
                            failedBatches++;
                            totalMigratedDocuments += (batch.Count - batchErrors);
                            Console.WriteLine($"✗ Batch {batchNumber}: {batchErrors} errors in {batch.Count} docs");
                        }

                        // Small delay between batches to avoid throttling
                        if (batchNumber % 10 == 0)
                        {
                            await Task.Delay(500);
                        }
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Error in batch {batchNumber}: {ex.Message}");
                        await Task.Delay(2000);
                    }
                }

                // Summary
                Console.WriteLine($"\n=== Migration Summary ===");
                Console.WriteLine($"Total documents: {totalDocuments:N0}");
                Console.WriteLine($"Successfully migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Failed batches: {failedBatches}");

                if (totalDocuments > 0)
                {
                    double successRate = (double)totalMigratedDocuments / totalDocuments * 100;
                    Console.WriteLine($"Success rate: {successRate:F2}%");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during data migration: {ex.Message}");
                throw;
            }
        }

    }
}
