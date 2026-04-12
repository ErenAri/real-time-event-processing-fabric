# Azure Container Apps Variant

This deployment path is the first Azure-hosted variant of PulseStream. It is designed to prove that the same Go services can run against Azure Event Hubs over the Kafka protocol and can be hosted as Azure Container Apps without code changes beyond environment configuration.

## What this variant includes

- Azure Container Apps environment with Log Analytics integration
- external ingress for `ingest-service` and `query-service`
- internal `stream-processor` with configurable replica limits
- Event Hubs Kafka endpoint configuration via `SASL_SSL` and `PLAIN`
- Blob-backed raw archive for replay durability
- system-assigned managed identity on `ingest-service` for Blob Storage access
- existing PostgreSQL and Event Hubs credentials injected as Container Apps secrets

## What this variant does not solve yet

- dashboard deployment parity. The current dashboard image bakes the API base URL at build time, so it should be rebuilt separately for the Azure query-service URL.
- infrastructure provisioning for Event Hubs, PostgreSQL, registry, or storage. This template assumes those already exist and focuses on application deployment.

## Required inputs

Populate [main.parameters.example.json](/C:/Projects/real-time-event-processing-fabric/deploy/azure/container-apps/main.parameters.example.json) with real values and then deploy with Azure CLI.

- `ingestImage`, `queryImage`, `processorImage`: published container images
- `postgresAppUrl`: runtime PostgreSQL connection string for the app role
- `postgresAdminUrl`: administrative PostgreSQL connection string for schema bootstrap
- `eventHubsHost`: Event Hubs namespace host without a port
- `eventHubsConnectionString`: Event Hubs namespace connection string used by the Kafka client with username `$ConnectionString`
- `blobArchiveAccountUrl`: Blob Storage account URL for the raw archive
- `blobArchiveContainer`, `blobArchivePrefix`: archive location inside Blob Storage
- `authJwtSecret`, `adminToken`: deployment secrets

## Deploy

```powershell
az deployment group create `
  --resource-group <resource-group> `
  --template-file deploy/azure/container-apps/main.bicep `
  --parameters @deploy/azure/container-apps/main.parameters.example.json
```

After deployment, assign `Storage Blob Data Contributor` to the `ingest-service` managed identity for the target storage account or container scope. The template outputs the principal ID as `ingestPrincipalId`.

## Runtime configuration

This deployment depends on the Kafka security settings now supported by the Go services:

- `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
- `KAFKA_SASL_MECHANISM=PLAIN`
- `KAFKA_SASL_USERNAME=$ConnectionString`
- `KAFKA_SASL_PASSWORD=<Event Hubs namespace connection string>`
- `KAFKA_TLS_SERVER_NAME=<namespace>.servicebus.windows.net`
- `KAFKA_ALLOW_AUTO_TOPIC_CREATION=false`
- `RAW_ARCHIVE_BACKEND=azure_blob`
- `RAW_ARCHIVE_AZURE_BLOB_ACCOUNT_URL=https://<account>.blob.core.windows.net/`
- `RAW_ARCHIVE_AZURE_BLOB_CONTAINER=<container>`
- `RAW_ARCHIVE_AZURE_BLOB_PREFIX=<prefix>`

The Event Hubs Kafka endpoint details come from Microsoft Learn: Event Hubs Kafka uses `SASL_SSL`, `PLAIN`, port `9093`, and the namespace connection string with username `$ConnectionString`.
