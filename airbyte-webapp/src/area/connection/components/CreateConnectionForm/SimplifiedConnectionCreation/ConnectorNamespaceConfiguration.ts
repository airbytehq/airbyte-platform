import { ConnectorIds } from "area/connector/utils";

export const SourceNamespaceConfiguration = {
  [ConnectorIds.Sources.EndToEndTesting]: { supportsNamespaces: false },
} as const;

export const DestinationNamespaceConfiguration = {
  [ConnectorIds.Destinations.BigQuery]: { supportsNamespaces: true, defaultNamespacePath: "dataset_id" },
  [ConnectorIds.Destinations.EndToEndTesting]: { supportsNamespaces: false },
  [ConnectorIds.Destinations.Milvus]: { supportsNamespaces: true },
  [ConnectorIds.Destinations.Pinecone]: { supportsNamespaces: true },
  [ConnectorIds.Destinations.Postgres]: { supportsNamespaces: true, defaultNamespacePath: "schema" },
  [ConnectorIds.Destinations.Redshift]: { supportsNamespaces: true, defaultNamespacePath: "schema" },
  [ConnectorIds.Destinations.S3]: { supportsNamespaces: true },
  [ConnectorIds.Destinations.Snowflake]: { supportsNamespaces: true, defaultNamespacePath: "schema" },
  [ConnectorIds.Destinations.Weaviate]: { supportsNamespaces: false },
} as const;
