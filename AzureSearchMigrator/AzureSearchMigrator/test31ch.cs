using Azure;
using Azure.Search.Documents;
using Azure.Search.Documents.Models;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using static Azure.Search.Documents.Indexes.Models.LexicalAnalyzerName;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace AzureSearchMigrator
{
    class Program
    {
        private static IConfiguration Configuration;

        static async Task Main(string[] args)
        {
            try
            {
                LoadConfiguration();

                var sourceServiceName = Configuration["SourceSearchService:ServiceName"];
                var sourceIndexName = Configuration["SourceSearchService:IndexName"];
                var sourceAdminApiKey = Configuration["SourceSearchService:AdminApiKey"];

                var targetServiceName = Configuration["TargetSearchService:ServiceName"];
                var targetIndexName = Configuration["TargetSearchService:IndexName"];
                var targetAdminApiKey = Configuration["TargetSearchService:AdminApiKey"];

                var batchSize = 1000;
                var apiVersion = Configuration["MigrationSettings:ApiVersion"] ?? "2023-11-01";

                if (string.IsNullOrEmpty(sourceServiceName) || string.IsNullOrEmpty(sourceAdminApiKey) ||
                    string.IsNullOrEmpty(targetServiceName) || string.IsNullOrEmpty(targetAdminApiKey))
                {
                    Console.WriteLine("ERROR: Please update appsettings.json with your Azure Search service details.");
                    Console.ReadKey();
                    return;
                }

                Console.WriteLine("Starting Azure Cognitive Search Migration...");
                Console.WriteLine($"Source: {sourceServiceName}/{sourceIndexName}");
                Console.WriteLine($"Target: {targetServiceName}/{targetIndexName}");
                Console.WriteLine($"Batch Size: {batchSize}");
                Console.WriteLine();

                // Step 1: Get index schema
                Console.WriteLine("Step 1: Retrieving index schema...");
                var indexSchema = await GetIndexSchemaAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                if (indexSchema == null)
                {
                    Console.WriteLine("Failed to retrieve index schema.");
                    return;
                }

                // Step 2: Create target index
                Console.WriteLine("Step 2: Creating target index...");
                await CreateTargetIndexAsync(targetServiceName, targetIndexName, targetAdminApiKey, indexSchema, apiVersion);

                // Step 3: Count documents
                Console.WriteLine("Step 3: Counting source documents...");
                int totalDocuments = await GetTotalDocumentCountViaApiAsync(sourceServiceName, sourceIndexName, sourceAdminApiKey, apiVersion);
                Console.WriteLine($"Total documents found: {totalDocuments:N0}");
                if (totalDocuments == 0) return;

                // Step 4: Migrate data
                Console.WriteLine("Step 4: Migrating all documents...");
                await MigrateAllDocumentsViaRestApiAsync(
                    sourceServiceName, sourceIndexName, sourceAdminApiKey,
                    targetServiceName, targetIndexName, targetAdminApiKey,
                    batchSize, totalDocuments, apiVersion);

                Console.WriteLine("\nMigration completed successfully!");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Migration failed: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
            }

            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }

        // ----------------- CONFIG -----------------
        static void LoadConfiguration()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
            Configuration = builder.Build();
        }

        // ----------------- SCHEMA -----------------
        static async Task<string> GetIndexSchemaAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            using var http = new HttpClient();
            var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
            http.DefaultRequestHeaders.Add("api-key", apiKey);
            var res = await http.GetAsync(url);
            if (!res.IsSuccessStatusCode)
            {
                Console.WriteLine($"Error retrieving schema: {res.StatusCode}");
                return null;
            }
            Console.WriteLine("Successfully retrieved index schema.");
            return await res.Content.ReadAsStringAsync();
        }

        static async Task CreateTargetIndexAsync(string serviceName, string indexName, string apiKey, string schema, string apiVersion)
        {
            using var http = new HttpClient();
            var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
            http.DefaultRequestHeaders.Add("api-key", apiKey);
            var content = new StringContent(schema, Encoding.UTF8, "application/json");
            var res = await http.PutAsync(url, content);
            if (!res.IsSuccessStatusCode)
            {
                var err = await res.Content.ReadAsStringAsync();
                Console.WriteLine($"Failed to create index: {err}");
                throw new Exception(err);
            }
            Console.WriteLine("Target index created successfully.");
        }

        // ----------------- COUNT -----------------
        static async Task<int> GetTotalDocumentCountViaApiAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            using var http = new HttpClient();
            var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/$count?api-version={apiVersion}";
            http.DefaultRequestHeaders.Add("api-key", apiKey);
            var res = await http.GetAsync(url);
            if (!res.IsSuccessStatusCode) return 0;
            var txt = await res.Content.ReadAsStringAsync();
            return int.TryParse(txt, out int count) ? count : 0;
        }

        // ----------------- MIGRATION -----------------
        static async Task MigrateAllDocumentsViaRestApiAsync(
            string sourceServiceName, string sourceIndexName, string sourceApiKey,
            string targetServiceName, string targetIndexName, string targetApiKey,
            int batchSize, int totalDocuments, string apiVersion)
        {
            int totalMigrated = 0, successfulBatches = 0, failedBatches = 0, batchNumber = 0;
            var start = DateTime.Now;

            KeyFieldInfo keyInfo = await GetKeyFieldInfoAsync(sourceServiceName, sourceIndexName, sourceApiKey, apiVersion);
            string lastKey = null;
            bool hasMore = true;

            Console.WriteLine($"\nKey Field: {keyInfo.Name} ({keyInfo.EdmType})");
            Console.WriteLine($"Estimated Batches: {Math.Ceiling(totalDocuments / (double)batchSize)}\n");

            while (hasMore && totalMigrated < totalDocuments)
            {
                batchNumber++;
                try
                {
                    var docs = await GetDocumentsWithCursorAsync(
                        sourceServiceName, sourceIndexName, sourceApiKey,
                        batchSize, keyInfo, lastKey, apiVersion);

                    if (docs.Count == 0)
                    {
                        hasMore = false;
                        break;
                    }

                    // Get next cursor key
                    var lastDoc = docs.Last();
                    if (lastDoc.TryGetValue(keyInfo.Name, out var keyValue) && keyValue != null)
                        lastKey = keyValue.ToString();

                    bool ok = await IndexDocumentsViaApiAsync(
                        targetServiceName, targetIndexName, targetApiKey,
                        docs, apiVersion);

                    if (ok)
                    {
                        successfulBatches++;
                        totalMigrated += docs.Count;
                        double pct = totalMigrated * 100.0 / totalDocuments;
                        Console.WriteLine($"✓ Batch {batchNumber,3}: {docs.Count,5} docs → {totalMigrated,8:N0}/{totalDocuments,8:N0} ({pct,5:F1}%)");
                    }
                    else
                    {
                        failedBatches++;
                        Console.WriteLine($"✗ Batch {batchNumber} failed to index.");
                    }

                    if (docs.Count < batchSize)
                        hasMore = false;
                }
                catch (Exception ex)
                {
                    failedBatches++;
                    Console.WriteLine($"Batch {batchNumber} failed: {ex.Message}");
                    await Task.Delay(2000);
                }

                if (batchNumber > 5000)
                {
                    Console.WriteLine("Safety limit reached.");
                    break;
                }
            }

            var totalTime = DateTime.Now - start;
            Console.WriteLine("\n" + new string('=', 70));
            Console.WriteLine("=== MIGRATION SUMMARY ===");
            Console.WriteLine($"Total: {totalDocuments:N0}");
            Console.WriteLine($"Migrated: {totalMigrated:N0}");
            Console.WriteLine($"Batches - Success: {successfulBatches}, Failed: {failedBatches}");
            Console.WriteLine($"Duration: {totalTime:hh\\:mm\\:ss}");
            Console.WriteLine($"Speed: {totalMigrated / Math.Max(totalTime.TotalSeconds, 1):F1} docs/s");
            Console.WriteLine(new string('=', 70));
        }

        // ----------------- KEY FIELD INFO -----------------
        class KeyFieldInfo
        {
            public string Name { get; set; }
            public string EdmType { get; set; }
            public bool IsString => string.Equals(EdmType, "Edm.String", StringComparison.OrdinalIgnoreCase) || string.IsNullOrEmpty(EdmType);
        }

        static async Task<KeyFieldInfo> GetKeyFieldInfoAsync(string serviceName, string indexName, string apiKey, string apiVersion)
        {
            try
            {
                using var http = new HttpClient();
                var url = $"https://{serviceName}.search.windows.net/indexes/{indexName}?api-version={apiVersion}";
                http.DefaultRequestHeaders.Add("api-key", apiKey);
                var res = await http.GetAsync(url);
                if (res.IsSuccessStatusCode)
                {
                    var schema = await res.Content.ReadAsStringAsync();
                    using var doc = JsonDocument.Parse(schema);
                    if (doc.RootElement.TryGetProperty("fields", out var fields))
                    {
                        foreach (var f in fields.EnumerateArray())
                        {
                            if (f.TryGetProperty("key", out var key) && key.GetBoolean())
                            {
                                string name = f.GetProperty("name").GetString();
                                string type = f.TryGetProperty("type", out var t) ? t.GetString() : "Edm.String";
                                return new KeyFieldInfo { Name = name, EdmType = type };
                            }
                        }
                    }
                }
            }
            catch { }
            Console.WriteLine("Defaulting to key 'id'.");
            return new KeyFieldInfo { Name = "id", EdmType = "Edm.String" };
        }

        // ----------------- PAGINATION -----------------
        static async Task<List<Dictionary<string, object>>> GetDocumentsWithCursorAsync(
            string serviceName, string indexName, string apiKey,
            int top, KeyFieldInfo keyInfo, string lastKey, string apiVersion)
        {
            var docs = new List<Dictionary<string, object>>();
            using var http = new HttpClient();
            var url = new StringBuilder($"https://{serviceName}.search.windows.net/indexes/{indexName}/docs?");
            url.Append($"api-version={apiVersion}&$top={top}&$orderby={Uri.EscapeDataString(keyInfo.Name)} asc&search=*");
            if (!string.IsNullOrEmpty(lastKey))
            {
                string filter = keyInfo.IsString
                    ? $"{keyInfo.Name} gt '{EscapeODataString(lastKey)}'"
                    : $"{keyInfo.Name} gt {lastKey}";
                url.Append($"&$filter={Uri.EscapeDataString(filter)}");
            }

            http.DefaultRequestHeaders.Add("api-key", apiKey);
            var res = await http.GetAsync(url.ToString());
            if (!res.IsSuccessStatusCode)
            {
                Console.WriteLine($"Failed to get docs: {res.StatusCode}");
                return docs;
            }

            var json = await res.Content.ReadAsStringAsync();
            using var doc = JsonDocument.Parse(json);
            if (doc.RootElement.TryGetProperty("value", out var arr))
            {
                foreach (var e in arr.EnumerateArray())
                {
                    var d = new Dictionary<string, object>();
                    foreach (var p in e.EnumerateObject())
                        d[p.Name] = GetValueFromJsonElement(p.Value);
                    docs.Add(d);
                }
            }
            return docs;
        }

        static string EscapeODataString(string value) =>
            value?.Replace("'", "''").Replace("\\", "\\\\").Replace("\"", "\\\"") ?? value;

        // ----------------- INDEXING -----------------
        static async Task<bool> IndexDocumentsViaApiAsync(
            string serviceName, string indexName, string apiKey,
            List<Dictionary<string, object>> documents, string apiVersion, int maxRetries = 2)
        {
            if (documents.Count == 0) return true;
            string url = $"https://{serviceName}.search.windows.net/indexes/{indexName}/docs/index?api-version={apiVersion}";

            for (int attempt = 1; attempt <= maxRetries + 1; attempt++)
            {
                try
                {
                    using var http = new HttpClient();
                    http.DefaultRequestHeaders.Add("api-key", apiKey);

                    var payload = new
                    {
                        value = documents.Select(d =>
                        {
                            var item = new Dictionary<string, object> { ["@search.action"] = "upload" };
                            foreach (var kv in d) item[kv.Key] = kv.Value;
                            return item;
                        }).ToList()
                    };

                    var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
                    {
                        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
                    });

                    var content = new StringContent(json, Encoding.UTF8, "application/json");
                    var res = await http.PostAsync(url, content);
                    var txt = await res.Content.ReadAsStringAsync();

                    if (!res.IsSuccessStatusCode)
                    {
                        Console.WriteLine($"Indexing HTTP {res.StatusCode}, attempt {attempt}");
                        if (attempt > maxRetries) return false;
                        await Task.Delay(500 * attempt);
                        continue;
                    }

                    // check per-doc failures
                    using var doc = JsonDocument.Parse(txt);
                    if (doc.RootElement.TryGetProperty("value", out var arr))
                    {
                        var failed = arr.EnumerateArray()
                            .Where(v => v.TryGetProperty("status", out var s) && (!s.TryGetInt32(out int st) || st >= 300))
                            .Count();
                        if (failed == 0) return true;
                        Console.WriteLine($"Partial failure: {failed} docs failed, retrying...");
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Indexing attempt {attempt} failed: {ex.Message}");
                    if (attempt > maxRetries) return false;
                    await Task.Delay(1000 * attempt);
                }
            }
            return false;
        }

        // ----------------- JSON VALUE -----------------
        static object GetValueFromJsonElement(JsonElement e) =>
            e.ValueKind switch
            {
                JsonValueKind.String => e.GetString(),
                JsonValueKind.Number => e.TryGetInt64(out var l) ? l :
                                        e.TryGetDouble(out var d) ? d : e.GetRawText(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Array => e.EnumerateArray().Select(GetValueFromJsonElement).ToList(),
                JsonValueKind.Object => e.EnumerateObject().ToDictionary(p => p.Name, p => GetValueFromJsonElement(p.Value)),
                _ => e.ToString()
            };
    }
}
