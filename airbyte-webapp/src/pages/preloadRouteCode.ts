import { ConnectionRoutePaths, DestinationPaths, RoutePaths, SourcePaths } from "pages/routePaths";

export type RouteCodeChunk =
  | "ConnectionsRoutes"
  | "AllConnectionsPage"
  | "ConnectionPage"
  | "StreamStatusPage"
  | "ConnectionTimelinePage"
  | "ConnectionReplicationPage"
  | "ConnectionSettingsPage"
  | "AllSourcesPage"
  | "SourceItemPage"
  | "SourceSettingsPage"
  | "SourceConnectionsPage"
  | "AllDestinationsPage"
  | "DestinationItemPage"
  | "DestinationSettingsPage"
  | "DestinationConnectionsPage";

export const ROUTE_CODE_MODULES: Record<RouteCodeChunk, string> = {
  ConnectionsRoutes: "pages/connections/ConnectionsRoutes",
  AllConnectionsPage: "pages/connections/AllConnectionsPage",
  ConnectionPage: "pages/connections/ConnectionPage",
  StreamStatusPage: "pages/connections/StreamStatusPage",
  ConnectionTimelinePage: "pages/connections/ConnectionTimelinePage",
  ConnectionReplicationPage: "pages/connections/ConnectionReplicationPage",
  ConnectionSettingsPage: "pages/connections/ConnectionSettingsPage",
  AllSourcesPage: "pages/source/AllSourcesPage",
  SourceItemPage: "pages/source/SourceItemPage",
  SourceSettingsPage: "pages/source/SourceSettingsPage",
  SourceConnectionsPage: "pages/source/SourceConnectionsPage",
  AllDestinationsPage: "pages/destination/AllDestinationsPage",
  DestinationItemPage: "pages/destination/DestinationItemPage",
  DestinationSettingsPage: "pages/destination/DestinationSettingsPage",
  DestinationConnectionsPage: "pages/destination/DestinationConnectionsPage",
};

interface RouteChunkFiles {
  js: string[];
  css: string[];
}

type RouteChunkMap = Record<string, RouteChunkFiles>;

const CONNECTION_ID = `(?!(?:${ConnectionRoutePaths.ConnectionNew})(?:/|$))[^/]+`;
const SOURCE_ID = `(?!(?:${SourcePaths.SelectSourceNew}|${SourcePaths.EnterpriseSource})(?:/|$))[^/]+`;
const DESTINATION_ID = `(?!(?:${DestinationPaths.SelectDestinationNew}|${DestinationPaths.EnterpriseDestination})(?:/|$))[^/]+`;

const ROUTE_CODE_PATHS: Array<{ pattern: RegExp; chunks: RouteCodeChunk[] }> = [
  {
    pattern: new RegExp(`^${RoutePaths.Connections}/?$`),
    chunks: ["ConnectionsRoutes", "AllConnectionsPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Connections}/${CONNECTION_ID}(?:/${ConnectionRoutePaths.Status})?/?$`),
    chunks: ["ConnectionsRoutes", "ConnectionPage", "StreamStatusPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Connections}/${CONNECTION_ID}/${ConnectionRoutePaths.Timeline}/?$`),
    chunks: ["ConnectionsRoutes", "ConnectionPage", "ConnectionTimelinePage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Connections}/${CONNECTION_ID}/${ConnectionRoutePaths.Replication}/?$`),
    chunks: ["ConnectionsRoutes", "ConnectionPage", "ConnectionReplicationPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Connections}/${CONNECTION_ID}/${ConnectionRoutePaths.Settings}/?$`),
    chunks: ["ConnectionsRoutes", "ConnectionPage", "ConnectionSettingsPage"],
  },
  {
    pattern: new RegExp(
      `^${RoutePaths.Connections}/${CONNECTION_ID}/(?:${ConnectionRoutePaths.Mappings}|${ConnectionRoutePaths.Transformation}|${ConnectionRoutePaths.JobHistory}|${ConnectionRoutePaths.DataActivationMappings})/?$`
    ),
    chunks: ["ConnectionsRoutes", "ConnectionPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Connections}(?:/.*)?/?$`),
    chunks: ["ConnectionsRoutes"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Source}/?$`),
    chunks: ["AllSourcesPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Source}/${SOURCE_ID}/?$`),
    chunks: ["SourceItemPage", "SourceSettingsPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Source}/${SOURCE_ID}/${SourcePaths.Connections}/?$`),
    chunks: ["SourceItemPage", "SourceConnectionsPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Destination}/?$`),
    chunks: ["AllDestinationsPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Destination}/${DESTINATION_ID}/?$`),
    chunks: ["DestinationItemPage", "DestinationSettingsPage"],
  },
  {
    pattern: new RegExp(`^${RoutePaths.Destination}/${DESTINATION_ID}/${DestinationPaths.Connections}/?$`),
    chunks: ["DestinationItemPage", "DestinationConnectionsPage"],
  },
];

const WORKSPACE_PATH_PREFIX = new RegExp(`^/${RoutePaths.Workspaces}/[^/]+/`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const isRouteChunkFiles = (value: unknown): value is RouteChunkFiles => {
  if (!isRecord(value)) {
    return false;
  }

  const hasStringJsFiles = Array.isArray(value.js) && value.js.every((href) => typeof href === "string");
  const hasStringCssFiles = Array.isArray(value.css) && value.css.every((href) => typeof href === "string");

  return hasStringJsFiles && hasStringCssFiles;
};

const parseRouteChunkMap = (json: string | null): RouteChunkMap | undefined => {
  if (!json) {
    return undefined;
  }

  try {
    const parsed: unknown = JSON.parse(json);
    if (!isRecord(parsed) || !Object.values(parsed).every(isRouteChunkFiles)) {
      return undefined;
    }

    return parsed as RouteChunkMap;
  } catch {
    return undefined;
  }
};

export const getRouteCodeChunks = (pathname: string): RouteCodeChunk[] => {
  const routePath = pathname.replace(WORKSPACE_PATH_PREFIX, "");
  if (routePath === pathname) {
    return [];
  }

  return ROUTE_CODE_PATHS.find(({ pattern }) => pattern.test(routePath))?.chunks ?? [];
};

export const preloadRouteCode = (pathname: string, doc: Document = document): void => {
  const script = doc.getElementById("airbyte-route-chunks");
  const routeChunkMap = parseRouteChunkMap(script?.textContent ?? null);
  if (!routeChunkMap) {
    return;
  }

  const jsHrefs = new Set<string>();
  const cssHrefs = new Set<string>();
  getRouteCodeChunks(pathname).forEach((chunk) => {
    const files = routeChunkMap[ROUTE_CODE_MODULES[chunk]];
    files?.js.forEach((href) => jsHrefs.add(href));
    files?.css.forEach((href) => cssHrefs.add(href));
  });

  jsHrefs.forEach((href) => {
    if (doc.querySelector(`link[rel="modulepreload"][href="${href}"]`)) {
      return;
    }

    const link = doc.createElement("link");
    link.rel = "modulepreload";
    link.href = href;
    link.crossOrigin = "";
    doc.head.append(link);
  });

  cssHrefs.forEach((href) => {
    if (doc.querySelector(`link[rel="preload"][as="style"][href="${href}"], link[rel="stylesheet"][href="${href}"]`)) {
      return;
    }

    const link = doc.createElement("link");
    link.rel = "preload";
    link.setAttribute("as", "style");
    link.href = href;
    link.crossOrigin = "";
    doc.head.append(link);
  });
};
