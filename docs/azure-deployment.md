# Azure Deployment

## Objective

This document describes the first Azure-hosted PulseStream variant. The goal is to prove that the backend services can run on Azure without changing the event-processing model: Event Hubs provides the Kafka endpoint, Azure Container Apps hosts the services, and PostgreSQL remains the operational store.

## Scope

Current Azure deployment coverage:

- `ingest-service` on Azure Container Apps with external ingress
- `query-service` on Azure Container Apps with external ingress
- `stream-processor` on Azure Container Apps without public ingress
- Event Hubs Kafka connectivity over `SASL_SSL`
- Blob-backed raw archive with replay scanning by day prefix
- system-assigned managed identity on `ingest-service` for Blob access
- Log Analytics wiring through the Container Apps environment

Not yet included:

- dashboard deployment with runtime-configurable API base URL
- infrastructure provisioning for Event Hubs, PostgreSQL, storage, or registry
- Azure benchmark and failure-drill artifacts

## Kafka compatibility settings

The Go services now support a generic Kafka transport configuration layer. For Azure Event Hubs, use:

- `KAFKA_BROKERS=<namespace>.servicebus.windows.net:9093`
- `KAFKA_SECURITY_PROTOCOL=SASL_SSL`
- `KAFKA_SASL_MECHANISM=PLAIN`
- `KAFKA_SASL_USERNAME=$ConnectionString`
- `KAFKA_SASL_PASSWORD=<Event Hubs namespace connection string>`
- `KAFKA_TLS_SERVER_NAME=<namespace>.servicebus.windows.net`
- `KAFKA_ALLOW_AUTO_TOPIC_CREATION=false`

This matches the Azure Event Hubs Kafka guidance from Microsoft Learn.

## Blob archive settings

The ingest service also supports an Azure Blob archive backend for durable replay storage:

- `RAW_ARCHIVE_BACKEND=azure_blob`
- `RAW_ARCHIVE_AZURE_BLOB_ACCOUNT_URL=https://<account>.blob.core.windows.net/`
- `RAW_ARCHIVE_AZURE_BLOB_CONTAINER=<container>`
- `RAW_ARCHIVE_AZURE_BLOB_PREFIX=<prefix>`

The preferred Azure path is managed identity with `DefaultAzureCredential`, not a storage connection string. The `ingest-service` container app therefore gets a system-assigned identity in the Bicep template. That identity must be granted `Storage Blob Data Contributor` on the storage account or container scope.

## Deployment files

- [main.bicep](/C:/Projects/real-time-event-processing-fabric/deploy/azure/container-apps/main.bicep): Container Apps environment, Log Analytics workspace, and backend app resources
- [main.parameters.example.json](/C:/Projects/real-time-event-processing-fabric/deploy/azure/container-apps/main.parameters.example.json): example deployment inputs
- [README.md](/C:/Projects/real-time-event-processing-fabric/deploy/azure/container-apps/README.md): deployment instructions and current limitations

## Deployment flow

1. Build and publish the service images.
2. Prepare existing Azure dependencies:
   `Event Hubs namespace and Event Hubs entities`
   `PostgreSQL database and credentials`
   `Blob Storage account and container`
3. Populate the deployment parameters file with real image names and secrets.
4. Deploy the Container Apps template with Azure CLI.
5. Assign `Storage Blob Data Contributor` to the `ingest-service` managed identity.
6. Validate ingress, service health, Event Hubs connectivity, and replay against Blob-backed archive data.
