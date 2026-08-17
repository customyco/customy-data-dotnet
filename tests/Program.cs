using System.Text;
using System.Text.Json.Nodes;
using Customy.Data;

sealed class Recorder(params int[] statuses)
{
    private readonly Queue<int> _statuses = new(statuses);
    public List<JsonObject> Bodies { get; } = [];

    public Task<DataResponse> Send(Uri url, IReadOnlyDictionary<string, string> headers, byte[] body,
        TimeSpan timeout, CancellationToken token)
    {
        var payload = JsonNode.Parse(body)!.AsObject();
        Bodies.Add((JsonObject)payload.DeepClone());
        var status = _statuses.Count == 0 ? 202 : _statuses.Dequeue();
        var count = payload["batch"] is JsonArray batch ? batch.Count : 1;
        var response = status < 300
            ? new JsonObject { ["accepted"] = count, ["deduplicated"] = 0, ["quarantined"] = 0, ["results"] = new JsonArray() }
            : new JsonObject { ["error"] = "temporary" };
        return Task.FromResult(new DataResponse(status, Encoding.UTF8.GetBytes(response.ToJsonString())));
    }
}

static class Program
{
    static CustomyDataClient Client(Recorder recorder, int maxRetries = 3, int maxBatchSize = 100,
        IEnumerable<string>? redact = null, Func<JsonObject, JsonObject?>? hook = null)
    {
        var id = 0;
        return new CustomyDataClient("https://data.customy.ai", "cdw_test", recorder.Send,
            maxRetries, TimeSpan.Zero, maxBatchSize: maxBatchSize, redactFields: redact,
            beforeSend: hook, now: () => DateTimeOffset.Parse("2026-08-16T00:00:00Z"),
            idFactory: () => $"message_{++id}");
    }

    static async Task Main()
    {
        var vectors = JsonNode.Parse(await File.ReadAllTextAsync("../sdk-data/conformance/customer-data-v1.json"))!.AsObject();
        Check(vectors["contract"]!.GetValue<string>() == CustomyDataClient.ConformanceContract, "contract");
        var recorder = new Recorder();
        var sdk = Client(recorder);
        foreach (var item in vectors["eventTypes"]!.AsArray()) await sdk.SendEventAsync(item!.AsObject());
        Check(recorder.Bodies.Select(item => item["type"]!.GetValue<string>())
            .SequenceEqual(new[] { "track", "identify", "group", "page", "screen", "alias" }), "six calls");
        Check(recorder.Bodies.All(item => item["schemaVersion"]!.GetValue<string>() == "1.0"), "schema version");

        var retry = new Recorder(503, 429, 202);
        await Client(retry).TrackAsync("Checkout Started", new JsonObject { ["value"] = 10 },
            new JsonObject { ["anonymousId"] = "anon_1" });
        Check(retry.Bodies.Select(item => item["messageId"]!.GetValue<string>()).Distinct().Count() == 1, "stable retry");

        var redaction = new Recorder();
        var redacting = Client(redaction, redact: ["password"], hook: item =>
        {
            item["traits"] = new JsonObject { ["password"] = "reintroduced" }; return item;
        });
        await redacting.IdentifyAsync(new JsonObject { ["password"] = "secret" }, new JsonObject { ["userId"] = "u1" });
        Check(redaction.Bodies[0]["traits"]!["password"]!.GetValue<string>() == "[REDACTED]", "redaction");
        Expect<ArgumentException>(() => redacting.Event(new JsonObject
            { ["type"] = "identify", ["userId"] = "u1", ["organizationId"] = "forged" }));

        var partial = Client(new Recorder(202, 503), maxRetries: 0, maxBatchSize: 2);
        foreach (var name in new[] { "A", "B", "C" }) partial.Enqueue(new JsonObject
            { ["type"] = "track", ["event"] = name, ["anonymousId"] = "anon_1" });
        try { await partial.FlushAsync(); throw new Exception("expected partial failure"); }
        catch (CustomyDataException) { }
        Check(partial.Enqueue(new JsonObject
            { ["type"] = "track", ["event"] = "D", ["anonymousId"] = "anon_1" }) == 4, "queue restore");
        Console.WriteLine("customy-data-dotnet conformance passed");
    }

    static void Check(bool condition, string label) { if (!condition) throw new Exception($"failed: {label}"); }
    static void Expect<T>(Action action) where T : Exception
    {
        try { action(); } catch (T) { return; }
        throw new Exception($"expected {typeof(T).Name}");
    }
}
