static async Task MigrateDataWithRangePaginationAsync(SearchClient sourceClient, SearchClient targetClient, int batchSize, int totalDocuments)
{
    int successfulBatches = 0;
    int failedBatches = 0;
    int totalMigratedDocuments = 0;

    try
    {
        Console.WriteLine($"Total documents to migrate: {totalDocuments:N0}");
        Console.WriteLine($"Batch size: {batchSize}");
        Console.WriteLine("Using range-based pagination to avoid $skip limits...");
        Console.WriteLine();

        // Get the key field range to partition the data
        var (minKey, maxKey) = await GetKeyFieldRangeAsync(sourceClient);
        
        if (minKey == null || maxKey == null)
        {
            Console.WriteLine("Could not determine key field range. Using sequential batch processing.");
            await MigrateDataSequentially(sourceClient, targetClient, batchSize, totalDocuments);
            return;
        }

        Console.WriteLine($"Key field range: {minKey} to {maxKey}");

        // Process in ranges to avoid skip limits
        int processedCount = 0;
        string currentRangeStart = minKey;

        while (processedCount < totalDocuments && !string.IsNullOrEmpty(currentRangeStart))
        {
            var batchResult = await ProcessRangeBatchAsync(sourceClient, targetClient, currentRangeStart, batchSize);
            
            if (batchResult.Success)
            {
                successfulBatches++;
                totalMigratedDocuments += batchResult.ProcessedCount;
                processedCount += batchResult.ProcessedCount;
                currentRangeStart = batchResult.NextRangeStart;

                double percentage = (double)totalMigratedDocuments / totalDocuments * 100;
                Console.WriteLine($"✓ Range Batch {successfulBatches}: {batchResult.ProcessedCount} documents - Total: {totalMigratedDocuments:N0}/{totalDocuments:N0} ({percentage:F1}%)");
            }
            else
            {
                failedBatches++;
                Console.WriteLine($"✗ Range Batch {successfulBatches + failedBatches}: Failed to process range starting at {currentRangeStart}");
                break;
            }

            // Small delay between batches
            await Task.Delay(100);
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

static async Task<(string MinKey, string MaxKey)> GetKeyFieldRangeAsync(SearchClient client)
{
    try
    {
        // First, try to get min and max of a key field
        var minOptions = new SearchOptions
        {
            Size = 1,
            OrderBy = { "keyField asc" } // Replace with your actual key field
        };

        var maxOptions = new SearchOptions
        {
            Size = 1,
            OrderBy = { "keyField desc" } // Replace with your actual key field
        };

        var minResults = await client.SearchAsync<SearchDocument>("*", minOptions);
        var maxResults = await client.SearchAsync<SearchDocument>("*", maxOptions);

        string minKey = null;
        string maxKey = null;

        await foreach (var result in minResults.GetResultsAsync())
        {
            if (result.Document.ContainsKey("keyField"))
            {
                minKey = result.Document["keyField"]?.ToString();
                break;
            }
        }

        await foreach (var result in maxResults.GetResultsAsync())
        {
            if (result.Document.ContainsKey("keyField"))
            {
                maxKey = result.Document["keyField"]?.ToString();
                break;
            }
        }

        return (minKey, maxKey);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error getting key field range: {ex.Message}");
        return (null, null);
    }
}
