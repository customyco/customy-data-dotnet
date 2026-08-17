# Conformance

This repository is exported from the canonical Customy monorepo and implements `dotnet` version `0.1.1` against `customy.customer-data-sdk.conformance.v1`.

The fixture in `conformance/customer-data-v1.json` verifies the six-call collection contract, stable idempotency identifiers, bounded retries, recursive redaction and rejection of tenant scope supplied by public payloads. Passing source tests is required for a release, but registry publication and GA remain separate gates.
