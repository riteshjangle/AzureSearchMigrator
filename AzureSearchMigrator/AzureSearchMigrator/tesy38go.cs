using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System.Net.Http;
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

                // Step 4: Migrate data using Continuation Tokens (Fixed Paging Logic)
                Console.WriteLine("Step 4: Migrating data...");
                await MigrateDataWithPagingAsync(sourceClient, targetClient, batchSize, totalDocuments);

                Console.WriteLine("\nMigration completed successfully! 🎉");
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
                // Remove read-only properties from the schema before sending (e.g., '@odata.context', 'documentCount')
                var jsonDoc = JsonDocument.Parse(indexSchema);
                var root = jsonDoc.RootElement.Clone();

                var newIndexProperties = new Dictionary<string, object>();
                foreach (var prop in root.EnumerateObject())
                {
                    if (prop.NameEquals("@odata.context") || prop.NameEquals("etag") || prop.NameEquals("documentCount") || prop.NameEquals("name"))
                    {
                        continue;
                    }
                    newIndexProperties.Add(prop.Name, prop.Value.Clone());
                }

                // Ensure the index 'name' property is set to the target index name
                newIndexProperties["name"] = indexName;
                var cleanSchema = JsonSerializer.Serialize(newIndexProperties, new JsonSerializerOptions { WriteIndented = true });

                using (var httpClient = new HttpClient())
                {
                    var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                    httpClient.DefaultRequestHeaders.Add("api-key", apiKey);

                    var content = new StringContent(cleanSchema, Encoding.UTF8, "application/json");
                    var response = await httpClient.PutAsync(url, content);

                    if (response.IsSuccessStatusCode || response.StatusCode == System.Net.HttpStatusCode.Created)
                    {
                        Console.WriteLine("Successfully created or updated target index.");
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
                // Use a search with IncludeTotalCount = true and Size = 0 to get just the count
                var searchOptions = new SearchOptions
                {
                    IncludeTotalCount = true,
                    Size = 0
                };

                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                long totalCount = results.TotalCount ?? 0;
                Console.WriteLine($"Total documents found: {totalCount:N0}");
                return (int)totalCount;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting document count: {ex.Message}");
                return 0;
            }
        }

        /// <summary>
        /// Corrected data migration using search continuation tokens (handled by GetResultsAsync())
        /// instead of $skip, which avoids the 100,000 document limit.
        /// </summary>
        static async Task MigrateDataWithPagingAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;

            try
            {
                Console.WriteLine($"Total documents to migrate: {totalDocuments:N0}");
                Console.WriteLine($"Batch size: {batchSize}");

                // 1. Define Search Options: Set Size to the desired batch size. Do NOT set Skip.
                var searchOptions = new SearchOptions
                {
                    Size = batchSize,
                    IncludeTotalCount = false
                };

                // 2. Perform the initial search
                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);

                // 3. Iterate over the results stream using GetResultsAsync()
                // This asynchronous enumerator automatically handles fetching the next page using continuation tokens.
                await using (var enumerator = results.GetResultsAsync().GetAsyncEnumerator())
                {
                    while (true)
                    {
                        var batch = new List<SearchDocument>();
                        bool hasMoreDocuments = false;

                        // Fill the batch from the stream up to batchSize
                        for (int i = 0; i < batchSize; i++)
                        {
                            if (await enumerator.MoveNextAsync())
                            {
                                batch.Add(enumerator.Current.Document);
                                hasMoreDocuments = true;
                            }
                            else
                            {
                                // End of the entire results stream
                                break;
                            }
                        }

                        if (batch.Count > 0)
                        {
                            try
                            {
                                // 4. Upload the batch
                                IndexDocumentsResult batchResult = await targetClient.IndexDocumentsAsync(
                                    IndexDocumentsBatch.Upload(batch));

                                // Check for errors
                                bool batchSuccess = true;
                                int batchErrors = 0;
                                foreach (IndexingResult item in batchResult.Results)
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

                                // Logging and counting logic
                                if (batchSuccess)
                                {
                                    successfulBatches++;
                                    totalMigratedDocuments += batch.Count;
                                    double percentage = totalDocuments > 0 ? (double)totalMigratedDocuments / totalDocuments * 100 : 0;
                                    Console.WriteLine($"✓ Batch {successfulBatches + failedBatches}: {batch.Count} documents - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
                                }
                                else
                                {
                                    failedBatches++;
                                    Console.WriteLine($"✗ Batch {successfulBatches + failedBatches}: {batchErrors} errors in {batch.Count} documents");
                                }
                            }
                            catch (Exception ex)
                            {
                                failedBatches++;
                                Console.WriteLine($"✗ Error uploading batch: {ex.Message}");
                                // Wait on network errors before continuing the enumeration
                                await Task.Delay(2000);
                            }
                        }

                        // If we didn't fill the batch, it means we hit the end of the entire dataset.
                        if (!hasMoreDocuments)
                        {
                            break;
                        }

                        // Small delay between batches to avoid throttling
                        if ((successfulBatches + failedBatches) % 10 == 0)
                        {
                            await Task.Delay(500);
                        }
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
