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

                // Step 1: Get index schema
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

                // Step 3: Get total count (optional)
                Console.WriteLine("Step 3: Counting total documents...");
                int totalDocuments = await GetTotalDocumentCountAsync(sourceClient);

                // Step 4: Migrate data using continuation tokens
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
                var options = new SearchOptions
                {
                    Size = 1,
                    IncludeTotalCount = true
                };

                var response = await sourceClient.SearchAsync<SearchDocument>("*", options);
                if (response != null && response.TotalCount.HasValue)
                {
                    Console.WriteLine($"Total documents found: {response.TotalCount.Value:N0}");
                    return (int)response.TotalCount.Value;
                }
                else
                {
                    Console.WriteLine("Could not determine total count. Will migrate until no more documents are found.");
                    return -1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count: {ex.Message}");
                Console.WriteLine("Will migrate until no more documents are found.");
                return -1;
            }
        }

        // ✅ Fixed version using continuation tokens
        static async Task MigrateDataWithContinuationAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;

            Console.WriteLine($"Estimated total documents: {(totalDocuments > 0 ? totalDocuments.ToString("N0") : "unknown")}");
            Console.WriteLine($"Batch size: {batchSize}");
            Console.WriteLine();

            string continuationToken = null;
            int batchNumber = 0;

            try
            {
                do
                {
                    var searchOptions = new SearchOptions
                    {
                        Size = batchSize,
                        IncludeTotalCount = false
                    };

                    var response = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions, cancellationToken: default);
                    continuationToken = response.Value.ContinuationToken;

                    var batch = new List<SearchDocument>();
                    await foreach (var result in response.Value.GetResultsAsync())
                    {
                        batch.Add(result.Document);
                    }

                    if (batch.Count == 0)
                        break;

                    batchNumber++;

                    try
                    {
                        IndexDocumentsResult uploadResult = await targetClient.IndexDocumentsAsync(IndexDocumentsBatch.Upload(batch));
                        int failedCount = uploadResult.Results.Count(r => !r.Succeeded);

                        if (failedCount == 0)
                        {
                            successfulBatches++;
                            totalMigratedDocuments += batch.Count;
                            double percent = totalDocuments > 0 ? (double)totalMigratedDocuments / totalDocuments * 100 : 0;
                            Console.WriteLine($"✓ Batch {batchNumber}: {batch.Count} docs - Total: {totalMigratedDocuments:N0} ({percent:F1}%)");
                        }
                        else
                        {
                            failedBatches++;
                            Console.WriteLine($"✗ Batch {batchNumber}: {failedCount} failed of {batch.Count}");
                        }
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Error uploading batch {batchNumber}: {ex.Message}");
                    }

                    await Task.Delay(300); // small pause to avoid throttling

                } while (!string.IsNullOrEmpty(continuationToken));

                Console.WriteLine("\n=== Migration Summary ===");
                Console.WriteLine($"Total migrated: {totalMigratedDocuments:N0}");
                Console.WriteLine($"Successful batches: {successfulBatches}");
                Console.WriteLine($"Failed batches: {failedBatches}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during data migration: {ex.Message}");
                throw;
            }
        }
    }
}
