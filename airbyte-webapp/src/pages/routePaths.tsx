export enum RoutePaths {
  AuthFlow = "/auth_flow",
  Root = "/",

  SpeakeasyRedirect = "speakeasy-redirect",
  Workspaces = "workspaces",
  Setup = "setup",
  Connections = "connections",
  Destination = "destination",
  Source = "source",
  Settings = "settings",
  ConnectorBuilder = "connector-builder",
}

export enum DestinationPaths {
  Root = ":destinationId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Connections = "connections",
  SelectDestinationNew = "new-destination",
  DestinationNew = "new-destination/:destinationDefinitionId",
}

export enum SourcePaths {
  Root = ":sourceId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Connections = "connections",
  SelectSourceNew = "new-source",
  SourceNew = "new-source/:sourceDefinitionId",
}
export const enum ConnectionRoutePaths {
  Root = ":connectionId/*",
  Status = "status",
  Transformation = "transformation",
  Replication = "replication",
  Settings = "settings",
  JobHistory = "job-history",
  ConnectionNew = "new-connection",
  Configure = "configure",
}

export enum SettingsRoutePaths {
  Account = "account",
  Destination = "destination",
  Source = "source",
  Configuration = "configuration",
  Notifications = "notifications",
  Metrics = "metrics",
  DataResidency = "data-residency",
  Workspace = "workspace",
  Organization = "organization",
  AccessManagement = "access-management",
  Applications = "applications",
}
