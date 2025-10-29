static async Task MigrateDataWithPagingAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments)
{
    int successfulBatches = 0;
    int failedBatches = 0;
    int totalMigratedDocuments = 0;

    Console.WriteLine($"Total documents to migrate (approx): {totalDocuments:N0}");
    Console.WriteLine($"Batch size: {batchSize}");
    Console.WriteLine();

    var searchOptions = new SearchOptions
    {
        Size = batchSize,
        IncludeTotalCount = false
    };

    string? continuationToken = null;
    int batchNumber = 0;

    try
    {
        do
        {
            // Get a page of results
            var response = await sourceClient.SearchAsync<SearchDocument>("*", searchOptions);
            var results = response.Value.GetResultsAsync();

            var batch = new List<SearchDocument>();

            await foreach (var result in results)
            {
                batch.Add(result.Document);
            }

            // Upload the batch
            if (batch.Count > 0)
            {
                batchNumber++;

                IndexDocumentsResult uploadResult = await targetClient.IndexDocumentsAsync(
                    IndexDocumentsBatch.Upload(batch));

                int failedCount = uploadResult.Results.Count(r => !r.Succeeded);

                if (failedCount == 0)
                {
                    successfulBatches++;
                    totalMigratedDocuments += batch.Count;
                    double percentage = totalDocuments > 0
                        ? (double)totalMigratedDocuments / totalDocuments * 100
                        : 0;
                    Console.WriteLine($"✓ Batch {batchNumber}: {batch.Count} documents - Total: {totalMigratedDocuments:N0} ({percentage:F1}%)");
                }
                else
                {
                    failedBatches++;
                    Console.WriteLine($"✗ Batch {batchNumber}: {failedCount} failed");
                }
            }

            // Get continuation token for next page
            continuationToken = response.Value.ContinuationToken;

            // Small pause to avoid throttling
            await Task.Delay(300);

        } while (!string.IsNullOrEmpty(continuationToken));

        Console.WriteLine("\n=== Migration Summary ===");
        Console.WriteLine($"Total migrated: {totalMigratedDocuments:N0}");
        Console.WriteLine($"Successful batches: {successfulBatches}");
        Console.WriteLine($"Failed batches: {failedBatches}");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error during migration: {ex.Message}");
        throw;
    }
}
