export enum RoutePaths {
  Root = "/",
  Login = "login",
  Workspaces = "workspaces",
  Setup = "setup",
  Connections = "connections",
  Destination = "destination",
  Source = "source",
  Settings = "settings",
  ConnectorBuilder = "connector-builder",
  EmbeddedWidget = "embedded-widget",
  EmbeddedOnboarding = "embedded",
  Organization = "organization",
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
  EnterpriseSource = "enterprise/:id",
  SelectSourceNew = "new-source",
  SourceNew = "new-source/:sourceDefinitionId",
}
export const enum ConnectionRoutePaths {
  Root = ":connectionId/*",
  Status = "status",
  Transformation = "transformation",
  Replication = "replication",
  Settings = "settings",
  JobHistory = "job-history", // deprecated, used to support legacy logs links
  ConnectionNew = "new-connection",
  Configure = "configure",
  ConfigureDataActivation = "configure-da",
  ConfigureContinued = "continued",
  Timeline = "timeline",
  Mappings = "mappings",
}

export enum SettingsRoutePaths {
  Account = "account",
  Advanced = "advanced",
  Destination = "destination",
  Source = "source",
  Configuration = "configuration",
  Notifications = "notifications",
  Metrics = "metrics",
  DataResidency = "data-residency",
  Workspace = "workspace",
  Organization = "organization",
  OrganizationMembers = "organizationMembers",
  WorkspaceMembers = "workspaceMembers",
  Applications = "applications",
  License = "license",
}
