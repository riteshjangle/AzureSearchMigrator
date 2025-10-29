static async Task MigrateDataWithSearchAfterAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments)
{
    int successfulBatches = 0;
    int failedBatches = 0;
    int totalMigratedDocuments = 0;
    string lastSortValue = null;

    try
    {
        Console.WriteLine($"Total documents to migrate: {totalDocuments:N0}");
        Console.WriteLine($"Batch size: {batchSize}");
        Console.WriteLine($"Estimated batches: {Math.Ceiling((double)totalDocuments / batchSize):N0}");
        Console.WriteLine();

        bool hasMoreDocuments = true;

        while (hasMoreDocuments)
        {
            try
            {
                var searchOptions = new SearchOptions
                {
                    Size = batchSize,
                    OrderBy = { "search.score()", "keyField asc" } // Use a unique sortable field
                };

                // If we have a last sort value, use search after
                if (!string.IsNullOrEmpty(lastSortValue))
                {
                    // For search after, we need to use the SearchAfter method
                    // This requires a different approach
                }

                SearchResults<SearchDocument> results = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
                var batch = new List<SearchDocument>();
                string lastKey = null;

                await foreach (SearchResult<SearchDocument> result in results.GetResultsAsync())
                {
                    batch.Add(result.Document);
                    
                    // Track the last document's key for pagination
                    if (result.Document.ContainsKey("keyField")) // Replace with your actual key field
                    {
                        lastKey = result.Document["keyField"]?.ToString();
                    }
                }

                if (batch.Count > 0)
                {
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
                        double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                        Console.WriteLine($"✓ Batch {successfulBatches}: {batch.Count} documents - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
                        
                        // Update last sort value for next batch
                        lastSortValue = lastKey;
                    }
                    else
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Batch {successfulBatches + failedBatches}: {batchErrors} errors in {batch.Count} documents");
                    }

                    // If we got fewer documents than batch size, we're done
                    if (batch.Count < batchSize)
                    {
                        hasMoreDocuments = false;
                    }
                }
                else
                {
                    hasMoreDocuments = false; // No more documents
                }
            }
            catch (Exception ex)
            {
                failedBatches++;
                Console.WriteLine($"✗ Error in batch {successfulBatches + failedBatches}: {ex.Message}");
                await Task.Delay(2000);
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
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error during data migration: {ex.Message}");
        throw;
    }
}
