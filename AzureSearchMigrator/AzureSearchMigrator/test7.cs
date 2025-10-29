using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
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
                var sourceIndexClient = CreateSearchIndexClient(sourceServiceName, sourceAdminApiKey);

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

                // Step 4: Find a suitable field for ordered pagination
                Console.WriteLine("Step 4: Finding suitable field for pagination...");
                string orderByField = await FindOrderByFieldAsync(sourceIndexClient, sourceIndexName);
                if (string.IsNullOrEmpty(orderByField))
                {
                    Console.WriteLine("WARNING: No suitable field found for ordered pagination. Using default 'id' field.");
                    orderByField = "id";
                }

                // Step 5: Migrate data with proper batching
                Console.WriteLine("Step 5: Migrating data...");
                await MigrateDataWithOrderByAsync(sourceClient, targetClient, batchSize, totalDocuments, orderByField);

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

        static SearchIndexClient CreateSearchIndexClient(string serviceName, string apiKey)
        {
            Uri endpoint = new Uri($"https://{serviceName}.search.windows.net");
            AzureKeyCredential credential = new AzureKeyCredential(apiKey);
            return new SearchIndexClient(endpoint, credential);
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

                // Access TotalCount directly
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

        static async Task<string> FindOrderByFieldAsync(SearchIndexClient indexClient, string indexName)
        {
            try
            {
                // Get the index to check available fields
                var index = await indexClient.GetIndexAsync(indexName);

                // Try to find a suitable field for ordering
                // Prefer key fields that are unique and sortable
                string[] preferredFields = { "id", "key", "documentId", "rowId", "uniqueId", "Id", "ID", "Key" };

                foreach (var field in index.Value.Fields)
                {
                    string fieldName = field.Name;
                    if (preferredFields.Contains(fieldName) && field.IsSortable == true)
                    {
                        Console.WriteLine($"Using preferred field '{fieldName}' for ordered pagination");
                        return fieldName;
                    }
                }

                // If no preferred fields found, look for any string field that is sortable
                foreach (var field in index.Value.Fields)
                {
                    if (field.IsSortable == true && field.Type == SearchFieldDataType.String)
                    {
                        Console.WriteLine($"Using sortable string field '{field.Name}' for ordered pagination");
                        return field.Name;
                    }
                }

                // Look for any sortable field
                foreach (var field in index.Value.Fields)
                {
                    if (field.IsSortable == true)
                    {
                        Console.WriteLine($"Using sortable field '{field.Name}' for ordered pagination");
                        return field.Name;
                    }
                }

                Console.WriteLine("No sortable fields found in the index.");
                return null;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not analyze index fields: {ex.Message}");
                return null;
            }
        }

        static async Task MigrateDataWithOrderByAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments, string orderByField)
        {
            int successfulBatches = 0;
            int failedBatches = 0;
            int totalMigratedDocuments = 0;
            string lastKey = null;

            try
            {
                Console.WriteLine($"Total documents to migrate: {totalDocuments:N0}");
                Console.WriteLine($"Batch size: {batchSize}");
                Console.WriteLine($"Order by field: {orderByField}");
                Console.WriteLine($"Estimated batches: {Math.Ceiling((double)totalDocuments / batchSize):N0}");
                Console.WriteLine("Using ordered pagination to bypass 100,000 skip limit...");
                Console.WriteLine();

                bool hasMoreDocuments = true;
                int consecutiveEmptyBatches = 0;

                while (hasMoreDocuments && consecutiveEmptyBatches < 3)
                {
                    var searchOptions = new SearchOptions
                    {
                        Size = batchSize,
                        IncludeTotalCount = false,
                        OrderBy = { $"{orderByField} asc" } // Always order by the chosen field
                    };

                    // Add filter for pagination - only if we have a lastKey
                    if (lastKey != null)
                    {
                        searchOptions.Filter = $"{orderByField} gt '{EscapeFilterValue(lastKey)}'";
                    }

                    try
                    {
                        SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                        var batch = new List<SearchDocument>();
                        string currentLastKey = null;

                        await foreach (SearchResult<SearchDocument> result in results.GetResultsAsync())
                        {
                            batch.Add(result.Document);

                            // Update the last key for the next batch
                            if (result.Document.TryGetValue(orderByField, out object keyValue))
                            {
                                currentLastKey = keyValue?.ToString();
                            }
                        }

                        if (batch.Count > 0)
                        {
                            consecutiveEmptyBatches = 0; // Reset counter

                            // Update lastKey for next iteration
                            lastKey = currentLastKey;

                            // Upload the batch
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
                                    if (batchErrors <= 3) // Show first 3 errors only
                                    {
                                        Console.WriteLine($"  Failed to index document {item.Key}: {item.ErrorMessage}");
                                    }
                                }
                            }

                            if (batchSuccess)
                            {
                                successfulBatches++;
                                totalMigratedDocuments += batch.Count;
                                double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                                Console.WriteLine($"✓ Batch {successfulBatches + failedBatches}: {batch.Count} documents - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
                            }
                            else
                            {
                                failedBatches++;
                                Console.WriteLine($"✗ Batch {successfulBatches + failedBatches}: {batchErrors} errors in {batch.Count} documents");
                            }

                            // Check if we have more documents
                            hasMoreDocuments = batch.Count == batchSize;
                        }
                        else
                        {
                            consecutiveEmptyBatches++;
                            Console.WriteLine($"No documents returned in batch {successfulBatches + failedBatches + 1}. Empty batch #{consecutiveEmptyBatches}");

                            if (consecutiveEmptyBatches >= 3)
                            {
                                Console.WriteLine("Too many consecutive empty batches. Stopping migration.");
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Error in batch {successfulBatches + failedBatches}: {ex.Message}");

                        // Wait before continuing
                        await Task.Delay(2000);

                        // If we keep failing, we might need to adjust the approach
                        if (failedBatches > 5)
                        {
                            Console.WriteLine("Too many consecutive failures. Stopping migration.");
                            break;
                        }
                    }

                    // Small delay between batches to avoid throttling
                    if ((successfulBatches + failedBatches) % 10 == 0)
                    {
                        await Task.Delay(500);
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

                if (totalMigratedDocuments < totalDocuments)
                {
                    Console.WriteLine($"WARNING: Some documents may not have been migrated ({totalDocuments - totalMigratedDocuments} missing)");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during data migration: {ex.Message}");
                throw;
            }
        }

        static string EscapeFilterValue(string value)
        {
            if (string.IsNullOrEmpty(value)) return value;

            // Escape single quotes for OData filter
            return value.Replace("'", "''");
        }
    }
}
