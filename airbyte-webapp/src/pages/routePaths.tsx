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
  SelectSourceNew = "new-source",
  SourceNew = "new-source/:sourceDefinitionId",
  SelectDestinationNew = "new-destination",

  DestinationNew = "new-destination/:destinationDefinitionId",

  ConnectorBuilder = "connector-builder",
}

export enum DestinationPaths {
  Root = ":destinationId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Settings = "settings",
  SelectNewDestination = "new-destination",
  NewDestination = "new-destination/:destinationDefinitionId",
}

export enum SourcePaths {
  Root = ":sourceId/*", // currently our tabs rely on this * wildcard to detect which tab is currently active
  Settings = "settings",
  SelectNewSource = "new-source",
  NewSource = "new-source/:sourceDefinitionId",
}
