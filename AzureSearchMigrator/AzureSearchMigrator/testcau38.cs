using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Indexes;
using Azure.Search.Documents.Indexes.Models;
using Azure.Search.Documents.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AzureSearchMigration
{
    public class SearchMigrationConfig
    {
        public string SourceServiceEndpoint { get; set; }
        public string SourceApiKey { get; set; }
        public string SourceIndexName { get; set; }
        
        public string TargetServiceEndpoint { get; set; }
        public string TargetApiKey { get; set; }
        public string TargetIndexName { get; set; }
        
        public int BatchSize { get; set; } = 1000;
        public int MaxDegreeOfParallelism { get; set; } = 4;
    }

    public class AzureSearchMigrator
    {
        private readonly SearchMigrationConfig _config;
        private readonly SearchIndexClient _sourceIndexClient;
        private readonly SearchIndexClient _targetIndexClient;

        public AzureSearchMigrator(SearchMigrationConfig config)
        {
            _config = config;
            
            _sourceIndexClient = new SearchIndexClient(
                new Uri(config.SourceServiceEndpoint),
                new AzureKeyCredential(config.SourceApiKey));
                
            _targetIndexClient = new SearchIndexClient(
                new Uri(config.TargetServiceEndpoint),
                new AzureKeyCredential(config.TargetApiKey));
        }

        public async Task MigrateIndexSchemaAsync()
        {
            Console.WriteLine("Migrating index schema...");
            
            var sourceIndex = await _sourceIndexClient.GetIndexAsync(_config.SourceIndexName);
            var indexDefinition = sourceIndex.Value;
            
            // Update the index name if different
            indexDefinition.Name = _config.TargetIndexName;
            
            try
            {
                await _targetIndexClient.CreateIndexAsync(indexDefinition);
                Console.WriteLine($"Index '{_config.TargetIndexName}' created successfully.");
            }
            catch (RequestFailedException ex) when (ex.Status == 409)
            {
                Console.WriteLine($"Index '{_config.TargetIndexName}' already exists. Updating...");
                await _targetIndexClient.CreateOrUpdateIndexAsync(indexDefinition);
            }
        }

        public async Task<long> MigrateDataAsync(IProgress<MigrationProgress> progress = null)
        {
            Console.WriteLine("Starting data migration...");
            
            var sourceClient = _sourceIndexClient.GetSearchClient(_config.SourceIndexName);
            var targetClient = _targetIndexClient.GetSearchClient(_config.TargetIndexName);
            
            long totalDocuments = 0;
            long migratedDocuments = 0;
            var allDocuments = new List<SearchDocument>();
            
            // Retrieve all documents from source
            var searchOptions = new SearchOptions
            {
                Size = _config.BatchSize,
                IncludeTotalCount = true
            };
            
            SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
            totalDocuments = results.TotalCount ?? 0;
            
            Console.WriteLine($"Total documents to migrate: {totalDocuments}");
            
            // Process in batches
            await foreach (var doc in results.GetResultsAsync())
            {
                allDocuments.Add(doc.Document);
                
                if (allDocuments.Count >= _config.BatchSize)
                {
                    await UploadBatchAsync(targetClient, allDocuments);
                    migratedDocuments += allDocuments.Count;
                    
                    progress?.Report(new MigrationProgress
                    {
                        TotalDocuments = totalDocuments,
                        MigratedDocuments = migratedDocuments,
                        PercentComplete = (double)migratedDocuments / totalDocuments * 100
                    });
                    
                    Console.WriteLine($"Migrated {migratedDocuments}/{totalDocuments} documents ({(double)migratedDocuments / totalDocuments * 100:F2}%)");
                    allDocuments.Clear();
                }
            }
            
            // Upload remaining documents
            if (allDocuments.Any())
            {
                await UploadBatchAsync(targetClient, allDocuments);
                migratedDocuments += allDocuments.Count;
                progress?.Report(new MigrationProgress
                {
                    TotalDocuments = totalDocuments,
                    MigratedDocuments = migratedDocuments,
                    PercentComplete = 100
                });
            }
            
            Console.WriteLine($"Migration completed! Total documents migrated: {migratedDocuments}");
            return migratedDocuments;
        }

        private async Task UploadBatchAsync(SearchClient client, List<SearchDocument> documents)
        {
            try
            {
                var batch = IndexDocumentsBatch.Upload(documents);
                IndexDocumentsResult result = await client.IndexDocumentsAsync(batch);
                
                var failedDocs = result.Results.Where(r => !r.Succeeded).ToList();
                if (failedDocs.Any())
                {
                    Console.WriteLine($"Warning: {failedDocs.Count} documents failed to upload.");
                    foreach (var failed in failedDocs)
                    {
                        Console.WriteLine($"  Failed Key: {failed.Key}, Error: {failed.ErrorMessage}");
                    }
                }
            }
            catch (RequestFailedException ex)
            {
                Console.WriteLine($"Error uploading batch: {ex.Message}");
                throw;
            }
        }

        public async Task MigrateWithContinuationAsync()
        {
            Console.WriteLine("Starting data migration with continuation support...");
            
            var sourceClient = _sourceIndexClient.GetSearchClient(_config.SourceIndexName);
            var targetClient = _targetIndexClient.GetSearchClient(_config.TargetIndexName);
            
            long migratedDocuments = 0;
            string continuationToken = null;
            
            do
            {
                var searchOptions = new SearchOptions
                {
                    Size = _config.BatchSize,
                    IncludeTotalCount = true
                };
                
                var results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                var documents = new List<SearchDocument>();
                
                await foreach (var result in results.GetResultsAsync())
                {
                    documents.Add(result.Document);
                }
                
                if (documents.Any())
                {
                    await UploadBatchAsync(targetClient, documents);
                    migratedDocuments += documents.Count;
                    Console.WriteLine($"Migrated {migratedDocuments} documents so far...");
                }
                
                // Note: Azure Cognitive Search doesn't support direct continuation tokens
                // For very large datasets, consider using a skip/top pattern or timestamp filtering
                
            } while (continuationToken != null);
            
            Console.WriteLine($"Migration completed! Total documents: {migratedDocuments}");
        }
    }

    public class MigrationProgress
    {
        public long TotalDocuments { get; set; }
        public long MigratedDocuments { get; set; }
        public double PercentComplete { get; set; }
    }

    // Example usage
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new SearchMigrationConfig
            {
                SourceServiceEndpoint = "https://source-service.search.windows.net",
                SourceApiKey = "YOUR_SOURCE_API_KEY",
                SourceIndexName = "source-index",
                
                TargetServiceEndpoint = "https://target-service.search.windows.net",
                TargetApiKey = "YOUR_TARGET_API_KEY",
                TargetIndexName = "target-index",
                
                BatchSize = 1000,
                MaxDegreeOfParallelism = 4
            };

            var migrator = new AzureSearchMigrator(config);
            
            try
            {
                // Step 1: Migrate the index schema
                await migrator.MigrateIndexSchemaAsync();
                
                // Step 2: Migrate the data with progress reporting
                var progress = new Progress<MigrationProgress>(p =>
                {
                    Console.WriteLine($"Progress: {p.PercentComplete:F2}% - {p.MigratedDocuments}/{p.TotalDocuments}");
                });
                
                await migrator.MigrateDataAsync(progress);
                
                Console.WriteLine("Migration completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }
        }
    }
}
