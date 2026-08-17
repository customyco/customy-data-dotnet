# Customy Data SDK for .NET

Dependency-free .NET 8 SDK for governed `track`, `identify`, `group`, `page`,
`screen` and `alias` collection.

```csharp
var data = new CustomyDataClient(
    "https://data.customy.ai",
    "cdw_your_source_write_key",
    redactFields: ["password", "cardNumber"]);

await data.TrackAsync(
    "Product Viewed",
    new JsonObject { ["sku"] = "A-1" },
    new JsonObject { ["anonymousId"] = "anon_123" });
```

The write key is the only tenant authority. The SDK rejects forged tenant
scope before and after `beforeSend`, applies recursive redaction after the
hook, keeps `messageId` stable across retries, bounds its queue and restores
pending events after partial batch failures. It writes to Customy Data only;
Customy Analytics consumes governed read models.
