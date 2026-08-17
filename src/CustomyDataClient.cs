using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Customy.Data;

public sealed record DataResponse(int StatusCode, byte[] Body);

public sealed class CustomyDataException(
    string message,
    int? statusCode = null,
    JsonObject? response = null,
    Exception? innerException = null) : Exception(message, innerException)
{
    public int? StatusCode { get; } = statusCode;
    public JsonObject? Response { get; } = response;
}

public delegate Task<DataResponse> DataTransport(
    Uri url,
    IReadOnlyDictionary<string, string> headers,
    byte[] body,
    TimeSpan timeout,
    CancellationToken cancellationToken);

public sealed class CustomyDataClient
{
    public const string Version = "0.1.0";
    public const string ConformanceContract = "customy.customer-data-sdk.conformance.v1";

    private static readonly HashSet<string> EventTypes =
        ["track", "identify", "group", "page", "screen", "alias"];
    private static readonly HashSet<string> ForbiddenTenantFields =
        ["tenantId", "organizationId", "projectId", "environmentId"];
    private static readonly HashSet<int> RetryableStatuses = [429, 500, 502, 503, 504];

    private readonly string _collectUrl;
    private readonly string _writeKey;
    private readonly DataTransport _transport;
    private readonly int _maxRetries;
    private readonly TimeSpan _retryBase;
    private readonly TimeSpan _timeout;
    private readonly int _maxBatchSize;
    private readonly int _maxQueueSize;
    private readonly HashSet<string> _redactFields;
    private readonly Func<JsonObject, JsonObject?>? _beforeSend;
    private readonly Func<DateTimeOffset> _now;
    private readonly Func<string> _idFactory;
    private readonly List<JsonObject> _queue = [];
    private readonly object _queueLock = new();
    private readonly SemaphoreSlim _flushLock = new(1, 1);
    private int _inFlightCount;

    public CustomyDataClient(
        string collectUrl,
        string writeKey,
        DataTransport? transport = null,
        int maxRetries = 3,
        TimeSpan? retryBase = null,
        TimeSpan? timeout = null,
        int maxBatchSize = 100,
        int maxQueueSize = 10_000,
        IEnumerable<string>? redactFields = null,
        Func<JsonObject, JsonObject?>? beforeSend = null,
        Func<DateTimeOffset>? now = null,
        Func<string>? idFactory = null)
    {
        _collectUrl = collectUrl.TrimEnd('/');
        _writeKey = writeKey;
        if (string.IsNullOrWhiteSpace(_collectUrl) || string.IsNullOrWhiteSpace(_writeKey))
            throw new ArgumentException("collectUrl and writeKey are required");
        _transport = transport ?? HttpTransportAsync;
        _maxRetries = Math.Max(0, maxRetries);
        _retryBase = retryBase ?? TimeSpan.FromMilliseconds(250);
        _timeout = timeout ?? TimeSpan.FromSeconds(10);
        _maxBatchSize = Math.Clamp(maxBatchSize, 1, 1_000);
        _maxQueueSize = Math.Max(1, maxQueueSize);
        _redactFields = new HashSet<string>(redactFields ?? []);
        _beforeSend = beforeSend;
        _now = now ?? (() => DateTimeOffset.UtcNow);
        _idFactory = idFactory ?? (() => Guid.NewGuid().ToString().ToLowerInvariant());
    }

    public int QueueSize { get { lock (_queueLock) return _queue.Count + _inFlightCount; } }

    public JsonObject Event(JsonObject input)
    {
        var normalized = Clone(input);
        RejectTenantFields(normalized);
        Validate(normalized);
        normalized.TryAdd("messageId", _idFactory());
        normalized.TryAdd("timestamp", _now().UtcDateTime.ToString("O"));
        normalized.TryAdd("schemaVersion", "1.0");
        normalized.TryAdd("properties", new JsonObject());
        normalized.TryAdd("traits", new JsonObject());
        normalized.TryAdd("consent", new JsonObject());
        var context = normalized["context"] is JsonObject value ? Clone(value) : new JsonObject();
        context["library"] = new JsonObject { ["name"] = "customy-data-dotnet", ["version"] = Version };
        normalized["context"] = context;
        normalized = (JsonObject)Redact(normalized)!;
        if (_beforeSend is not null)
        {
            normalized = _beforeSend(Clone(normalized))
                ?? throw new CustomyDataException("event blocked by beforeSend");
            normalized = Clone(normalized);
            RejectTenantFields(normalized);
            Validate(normalized);
            normalized = (JsonObject)Redact(normalized)!;
        }
        return normalized;
    }

    public Task<JsonObject> SendEventAsync(JsonObject input, CancellationToken token = default) =>
        RequestAsync("event", Event(input), token);

    public Task<JsonObject> TrackAsync(string name, JsonObject properties, JsonObject identity, CancellationToken token = default) =>
        SendEventAsync(Compose(identity, "track", ("event", name), ("properties", properties)), token);

    public Task<JsonObject> IdentifyAsync(JsonObject traits, JsonObject identity, CancellationToken token = default) =>
        SendEventAsync(Compose(identity, "identify", ("traits", traits)), token);

    public Task<JsonObject> GroupAsync(JsonObject traits, JsonObject identity, CancellationToken token = default) =>
        SendEventAsync(Compose(identity, "group", ("traits", traits)), token);

    public Task<JsonObject> PageAsync(JsonObject properties, JsonObject identity, CancellationToken token = default) =>
        SendEventAsync(Compose(identity, "page", ("properties", properties)), token);

    public Task<JsonObject> ScreenAsync(JsonObject properties, JsonObject identity, CancellationToken token = default) =>
        SendEventAsync(Compose(identity, "screen", ("properties", properties)), token);

    public Task<JsonObject> AliasAsync(string userId, string previousId, JsonObject? identity = null, CancellationToken token = default) =>
        SendEventAsync(Compose(identity ?? [], "alias", ("userId", userId), ("anonymousId", previousId),
            ("properties", new JsonObject { ["previousId"] = previousId })), token);

    public int Enqueue(JsonObject input)
    {
        var normalized = Event(input);
        lock (_queueLock)
        {
            if (_queue.Count + _inFlightCount >= _maxQueueSize)
                throw new CustomyDataException("customer data queue is full");
            _queue.Add(normalized);
            return _queue.Count + _inFlightCount;
        }
    }

    public async Task<JsonObject> FlushAsync(CancellationToken token = default)
    {
        if (!await _flushLock.WaitAsync(0, token))
            throw new CustomyDataException("a customer data flush is already in progress");
        List<JsonObject> pending;
        lock (_queueLock)
        {
            pending = [.. _queue.Select(Clone)];
            _queue.Clear();
            _inFlightCount = pending.Count;
        }
        try
        {
            if (pending.Count == 0) return EmptyBatch();
            var aggregate = EmptyBatch();
            for (var offset = 0; offset < pending.Count; offset += _maxBatchSize)
            {
                var batch = new JsonArray(pending.Skip(offset).Take(_maxBatchSize).Select(item => (JsonNode?)Clone(item)).ToArray());
                var response = await RequestAsync("batch", new JsonObject { ["batch"] = batch }, token);
                foreach (var key in new[] { "accepted", "deduplicated", "quarantined" })
                    aggregate[key] = Number(aggregate[key]) + Number(response[key]);
                if (response["results"] is JsonArray results)
                    foreach (var result in results) ((JsonArray)aggregate["results"]!).Add(result?.DeepClone());
            }
            lock (_queueLock) _inFlightCount = 0;
            return aggregate;
        }
        catch
        {
            lock (_queueLock)
            {
                _queue.InsertRange(0, pending.Select(Clone));
                _inFlightCount = 0;
            }
            throw;
        }
        finally { _flushLock.Release(); }
    }

    private async Task<JsonObject> RequestAsync(string path, JsonObject payload, CancellationToken token)
    {
        var body = Encoding.UTF8.GetBytes(payload.ToJsonString());
        var headers = new Dictionary<string, string>
        {
            ["content-type"] = "application/json",
            ["user-agent"] = $"customy-data-dotnet/{Version}",
            ["x-write-key"] = _writeKey,
        };
        Exception? lastError = null;
        for (var attempt = 0; attempt <= _maxRetries; attempt++)
        {
            try
            {
                var response = await _transport(new Uri($"{_collectUrl}/v1/collect/{path}"), headers, body, _timeout, token);
                var parsed = Parse(response.Body);
                if (response.StatusCode is >= 200 and < 300) return parsed;
                throw new CustomyDataException(
                    $"Customy Data collection failed with HTTP {response.StatusCode}", response.StatusCode, parsed);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                lastError = error;
                if (attempt >= _maxRetries || !Retryable(error))
                {
                    if (error is CustomyDataException) throw;
                    throw new CustomyDataException($"Customy Data collection failed: {error.Message}", innerException: error);
                }
                await Task.Delay(_retryBase * (1 << attempt), token);
            }
        }
        throw new CustomyDataException($"Customy Data collection failed: {lastError?.Message}", innerException: lastError);
    }

    private static async Task<DataResponse> HttpTransportAsync(Uri url, IReadOnlyDictionary<string, string> headers,
        byte[] body, TimeSpan timeout, CancellationToken token)
    {
        using var client = new HttpClient { Timeout = timeout };
        using var request = new HttpRequestMessage(HttpMethod.Post, url) { Content = new ByteArrayContent(body) };
        foreach (var (key, value) in headers)
        {
            if (key == "content-type") request.Content.Headers.ContentType = MediaTypeHeaderValue.Parse(value);
            else request.Headers.TryAddWithoutValidation(key, value);
        }
        using var response = await client.SendAsync(request, token);
        return new DataResponse((int)response.StatusCode, await response.Content.ReadAsByteArrayAsync(token));
    }

    private void Validate(JsonObject value)
    {
        var type = String(value["type"]);
        if (type is null || !EventTypes.Contains(type))
            throw new ArgumentException("type must be track, identify, group, page, screen or alias");
        if (!new[] { "userId", "anonymousId", "groupId" }.Any(key => Present(value[key])))
            throw new ArgumentException("at least one userId, anonymousId or groupId is required");
        if (type == "track" && !Present(value["event"])) throw new ArgumentException("track calls require an event name");
    }

    private static void RejectTenantFields(JsonObject value)
    {
        var found = ForbiddenTenantFields.Where(value.ContainsKey).Order().ToArray();
        if (found.Length > 0)
            throw new ArgumentException($"tenant scope is derived from the write key; forbidden fields: {string.Join(", ", found)}");
    }

    private JsonNode? Redact(JsonNode? value)
    {
        if (value is JsonObject objectValue)
        {
            var result = new JsonObject();
            foreach (var (key, child) in objectValue)
                result[key] = _redactFields.Contains(key) ? "[REDACTED]" : Redact(child);
            return result;
        }
        if (value is JsonArray arrayValue) return new JsonArray(arrayValue.Select(Redact).ToArray());
        return value?.DeepClone();
    }

    private static JsonObject Compose(JsonObject identity, string type, params (string Key, JsonNode? Value)[] values)
    {
        var result = Clone(identity);
        result["type"] = type;
        foreach (var (key, value) in values) result[key] = value?.DeepClone();
        return result;
    }

    private static JsonObject Clone(JsonObject value) => (JsonObject)value.DeepClone();
    private static JsonObject Parse(byte[] body) => body.Length == 0 ? [] :
        JsonNode.Parse(body) as JsonObject ?? throw new JsonException("expected a JSON object");
    private static string? String(JsonNode? value) => value is JsonValue json && json.TryGetValue<string>(out var text) ? text : null;
    private static bool Present(JsonNode? value) => value is not null && (String(value) is not string text || text.Length > 0);
    private static int Number(JsonNode? value) => value is JsonValue json && json.TryGetValue<int>(out var number) ? number : 0;
    private static bool Retryable(Exception error) => error is not CustomyDataException customy ||
        customy.StatusCode is null || RetryableStatuses.Contains(customy.StatusCode.Value);
    private static JsonObject EmptyBatch() => new()
    {
        ["accepted"] = 0,
        ["deduplicated"] = 0,
        ["quarantined"] = 0,
        ["results"] = new JsonArray(),
    };
}
