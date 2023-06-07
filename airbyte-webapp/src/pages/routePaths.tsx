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
  Connection = "connection",
  ConnectionNew = "new-connection",
  ConnectorBuilder = "connector-builder",
}

export enum DestinationPaths {
  Root = ":destinationId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Settings = "settings",
  SelectDestinationNew = "new-destination",
  DestinationNew = "new-destination/:destinationDefinitionId",
}

export enum SourcePaths {
  Root = ":sourceId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Settings = "settings",
  SelectSourceNew = "new-source",
  SourceNew = "new-source/:sourceDefinitionId",
}
