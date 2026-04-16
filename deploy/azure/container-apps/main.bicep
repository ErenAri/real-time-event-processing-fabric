targetScope = 'resourceGroup'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Shared prefix used for Azure resource names.')
param namePrefix string = 'pulsestream'

@description('Log Analytics workspace name.')
param logAnalyticsWorkspaceName string = '${namePrefix}-logs'

@description('Container Apps environment name.')
param containerAppsEnvironmentName string = '${namePrefix}-env'

@description('Container app name for the ingest service.')
param ingestAppName string = '${namePrefix}-ingest'

@description('Container app name for the query service.')
param queryAppName string = '${namePrefix}-query'

@description('Container app name for the stream processor.')
param processorAppName string = '${namePrefix}-processor'

@description('Container image for the ingest service.')
param ingestImage string

@description('Container image for the query service.')
param queryImage string

@description('Container image for the stream processor.')
param processorImage string

@secure()
@description('Application connection string that services use for PostgreSQL runtime access.')
param postgresAppUrl string

@secure()
@description('Administrative PostgreSQL connection string used for schema bootstrap and role setup.')
param postgresAdminUrl string

@description('Event Hubs namespace host name without a port, for example mynamespace.servicebus.windows.net.')
param eventHubsHost string

@secure()
@description('Event Hubs namespace connection string used with the Kafka endpoint and SASL/PLAIN.')
param eventHubsConnectionString string

@description('Kafka topic or Event Hub name for accepted telemetry.')
param kafkaTopic string = 'pulsestream.events'

@description('Kafka topic or Event Hub name for processor dead letters.')
param kafkaDlqTopic string = 'pulsestream.events.dlq'

@description('Kafka consumer group used by stream-processor.')
param kafkaGroupId string = 'pulsestream-processor'

@secure()
@description('JWT signing secret for ingest and query services.')
param authJwtSecret string

@description('JWT issuer for Azure deployments.')
param authJwtIssuer string = 'pulsestream-azure'

@description('JWT audience for Azure deployments.')
param authJwtAudience string = 'pulsestream-azure'

@secure()
@description('Admin token for replay and local break-glass operations.')
param adminToken string

@description('Blob Storage account URL used for the raw archive, for example https://account.blob.core.windows.net/.')
param blobArchiveAccountUrl string

@description('Blob container used for archived raw events.')
param blobArchiveContainer string = 'raw-archive'

@description('Blob prefix used for archived raw events.')
param blobArchivePrefix string = 'raw-archive'

@description('Replica count floor for ingest-service.')
param ingestMinReplicas int = 1

@description('Replica count ceiling for ingest-service.')
param ingestMaxReplicas int = 2

@description('Replica count floor for query-service.')
param queryMinReplicas int = 1

@description('Replica count ceiling for query-service.')
param queryMaxReplicas int = 2

@description('Replica count floor for stream-processor.')
param processorMinReplicas int = 1

@description('Replica count ceiling for stream-processor.')
param processorMaxReplicas int = 3

@description('CPU allocation for ingest-service.')
param ingestCpu string = '0.5'

@description('Memory allocation for ingest-service.')
param ingestMemory string = '1Gi'

@description('CPU allocation for query-service.')
param queryCpu string = '0.5'

@description('Memory allocation for query-service.')
param queryMemory string = '1Gi'

@description('CPU allocation for stream-processor.')
param processorCpu string = '0.75'

@description('Memory allocation for stream-processor.')
param processorMemory string = '1.5Gi'

resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: logAnalyticsWorkspaceName
  location: location
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

resource containerAppsEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: containerAppsEnvironmentName
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsWorkspace.properties.customerId
        sharedKey: logAnalyticsWorkspace.listKeys().primarySharedKey
      }
    }
  }
}

resource ingestApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: ingestAppName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8080
        transport: 'auto'
      }
      secrets: [
        {
          name: 'postgres-app-url'
          value: postgresAppUrl
        }
        {
          name: 'postgres-admin-url'
          value: postgresAdminUrl
        }
        {
          name: 'eventhubs-connection-string'
          value: eventHubsConnectionString
        }
        {
          name: 'auth-jwt-secret'
          value: authJwtSecret
        }
        {
          name: 'admin-token'
          value: adminToken
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'ingest-service'
          image: ingestImage
          env: [
            {
              name: 'INGEST_LISTEN_ADDR'
              value: ':8080'
            }
            {
              name: 'POSTGRES_URL'
              secretRef: 'postgres-app-url'
            }
            {
              name: 'POSTGRES_ADMIN_URL'
              secretRef: 'postgres-admin-url'
            }
            {
              name: 'KAFKA_BROKERS'
              value: '${eventHubsHost}:9093'
            }
            {
              name: 'KAFKA_TOPIC'
              value: kafkaTopic
            }
            {
              name: 'KAFKA_SECURITY_PROTOCOL'
              value: 'SASL_SSL'
            }
            {
              name: 'KAFKA_SASL_MECHANISM'
              value: 'PLAIN'
            }
            {
              name: 'KAFKA_SASL_USERNAME'
              value: '\$ConnectionString'
            }
            {
              name: 'KAFKA_SASL_PASSWORD'
              secretRef: 'eventhubs-connection-string'
            }
            {
              name: 'KAFKA_TLS_SERVER_NAME'
              value: eventHubsHost
            }
            {
              name: 'KAFKA_ALLOW_AUTO_TOPIC_CREATION'
              value: 'false'
            }
            {
              name: 'RAW_ARCHIVE_BACKEND'
              value: 'azure_blob'
            }
            {
              name: 'RAW_ARCHIVE_AZURE_BLOB_ACCOUNT_URL'
              value: blobArchiveAccountUrl
            }
            {
              name: 'RAW_ARCHIVE_AZURE_BLOB_CONTAINER'
              value: blobArchiveContainer
            }
            {
              name: 'RAW_ARCHIVE_AZURE_BLOB_PREFIX'
              value: blobArchivePrefix
            }
            {
              name: 'RAW_ARCHIVE_FLUSH_INTERVAL'
              value: '5s'
            }
            {
              name: 'RAW_ARCHIVE_FLUSH_BYTES'
              value: '262144'
            }
            {
              name: 'AUTH_JWT_SECRET'
              secretRef: 'auth-jwt-secret'
            }
            {
              name: 'AUTH_JWT_ISSUER'
              value: authJwtIssuer
            }
            {
              name: 'AUTH_JWT_AUDIENCE'
              value: authJwtAudience
            }
            {
              name: 'ADMIN_TOKEN'
              secretRef: 'admin-token'
            }
          ]
          resources: {
            cpu: json(ingestCpu)
            memory: ingestMemory
          }
        }
      ]
      scale: {
        minReplicas: ingestMinReplicas
        maxReplicas: ingestMaxReplicas
      }
    }
  }
}

resource queryApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: queryAppName
  location: location
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8081
        transport: 'auto'
      }
      secrets: [
        {
          name: 'postgres-app-url'
          value: postgresAppUrl
        }
        {
          name: 'postgres-admin-url'
          value: postgresAdminUrl
        }
        {
          name: 'auth-jwt-secret'
          value: authJwtSecret
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'query-service'
          image: queryImage
          env: [
            {
              name: 'QUERY_LISTEN_ADDR'
              value: ':8081'
            }
            {
              name: 'POSTGRES_URL'
              secretRef: 'postgres-app-url'
            }
            {
              name: 'POSTGRES_ADMIN_URL'
              secretRef: 'postgres-admin-url'
            }
            {
              name: 'AUTH_JWT_SECRET'
              secretRef: 'auth-jwt-secret'
            }
            {
              name: 'AUTH_JWT_ISSUER'
              value: authJwtIssuer
            }
            {
              name: 'AUTH_JWT_AUDIENCE'
              value: authJwtAudience
            }
          ]
          resources: {
            cpu: json(queryCpu)
            memory: queryMemory
          }
        }
      ]
      scale: {
        minReplicas: queryMinReplicas
        maxReplicas: queryMaxReplicas
      }
    }
  }
}

resource processorApp 'Microsoft.App/containerApps@2024-03-01' = {
  name: processorAppName
  location: location
  properties: {
    environmentId: containerAppsEnvironment.id
    configuration: {
      activeRevisionsMode: 'Single'
      secrets: [
        {
          name: 'postgres-app-url'
          value: postgresAppUrl
        }
        {
          name: 'postgres-admin-url'
          value: postgresAdminUrl
        }
        {
          name: 'eventhubs-connection-string'
          value: eventHubsConnectionString
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'stream-processor'
          image: processorImage
          env: [
            {
              name: 'PROCESSOR_LISTEN_ADDR'
              value: ':8082'
            }
            {
              name: 'POSTGRES_URL'
              secretRef: 'postgres-app-url'
            }
            {
              name: 'POSTGRES_ADMIN_URL'
              secretRef: 'postgres-admin-url'
            }
            {
              name: 'KAFKA_BROKERS'
              value: '${eventHubsHost}:9093'
            }
            {
              name: 'KAFKA_TOPIC'
              value: kafkaTopic
            }
            {
              name: 'KAFKA_DLQ_TOPIC'
              value: kafkaDlqTopic
            }
            {
              name: 'KAFKA_GROUP_ID'
              value: kafkaGroupId
            }
            {
              name: 'KAFKA_SECURITY_PROTOCOL'
              value: 'SASL_SSL'
            }
            {
              name: 'KAFKA_SASL_MECHANISM'
              value: 'PLAIN'
            }
            {
              name: 'KAFKA_SASL_USERNAME'
              value: '\$ConnectionString'
            }
            {
              name: 'KAFKA_SASL_PASSWORD'
              secretRef: 'eventhubs-connection-string'
            }
            {
              name: 'KAFKA_TLS_SERVER_NAME'
              value: eventHubsHost
            }
            {
              name: 'KAFKA_ALLOW_AUTO_TOPIC_CREATION'
              value: 'false'
            }
          ]
          resources: {
            cpu: json(processorCpu)
            memory: processorMemory
          }
        }
      ]
      scale: {
        minReplicas: processorMinReplicas
        maxReplicas: processorMaxReplicas
      }
    }
  }
}

output ingestUrl string = 'https://${ingestApp.properties.latestRevisionFqdn}'
output queryUrl string = 'https://${queryApp.properties.latestRevisionFqdn}'
output containerAppsEnvironmentId string = containerAppsEnvironment.id
output ingestPrincipalId string = ingestApp.identity.principalId
