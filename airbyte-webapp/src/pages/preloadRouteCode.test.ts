import fs from "node:fs";
import path from "node:path";

import { ConnectionRoutePaths } from "pages/routePaths";

import { getRouteCodeChunks, preloadRouteCode, ROUTE_CODE_MODULES, type RouteCodeChunk } from "./preloadRouteCode";

const ROUTE_CODE_CASES: Array<[string, RouteCodeChunk[]]> = [
  ["/workspaces/abc/connections", ["ConnectionsRoutes", "AllConnectionsPage"]],
  ["/workspaces/abc/connections/", ["ConnectionsRoutes", "AllConnectionsPage"]],
  ["/workspaces/abc/connections/connection-id", ["ConnectionsRoutes", "ConnectionPage", "StreamStatusPage"]],
  ["/workspaces/abc/connections/connection-id/status", ["ConnectionsRoutes", "ConnectionPage", "StreamStatusPage"]],
  [
    "/workspaces/abc/connections/connection-id/timeline",
    ["ConnectionsRoutes", "ConnectionPage", "ConnectionTimelinePage"],
  ],
  [
    "/workspaces/abc/connections/connection-id/replication",
    ["ConnectionsRoutes", "ConnectionPage", "ConnectionReplicationPage"],
  ],
  [
    "/workspaces/abc/connections/connection-id/settings",
    ["ConnectionsRoutes", "ConnectionPage", "ConnectionSettingsPage"],
  ],
  [
    `/workspaces/abc/connections/connection-id/${ConnectionRoutePaths.Mappings}`,
    ["ConnectionsRoutes", "ConnectionPage"],
  ],
  [
    `/workspaces/abc/connections/connection-id/${ConnectionRoutePaths.Transformation}`,
    ["ConnectionsRoutes", "ConnectionPage"],
  ],
  [
    `/workspaces/abc/connections/connection-id/${ConnectionRoutePaths.JobHistory}`,
    ["ConnectionsRoutes", "ConnectionPage"],
  ],
  [
    `/workspaces/abc/connections/connection-id/${ConnectionRoutePaths.DataActivationMappings}`,
    ["ConnectionsRoutes", "ConnectionPage"],
  ],
  [`/workspaces/abc/connections/${ConnectionRoutePaths.ConnectionNew}`, ["ConnectionsRoutes"]],
  [
    `/workspaces/abc/connections/${ConnectionRoutePaths.ConnectionNew}/${ConnectionRoutePaths.Configure}`,
    ["ConnectionsRoutes"],
  ],
  ["/workspaces/abc/source", ["AllSourcesPage"]],
  ["/workspaces/abc/source/source-id", ["SourceItemPage", "SourceSettingsPage"]],
  ["/workspaces/abc/source/source-id/connections", ["SourceItemPage", "SourceConnectionsPage"]],
  ["/workspaces/abc/source/new-source", []],
  ["/workspaces/abc/source/new-source/source-definition-id", []],
  ["/workspaces/abc/source/enterprise/source-id", []],
  ["/workspaces/abc/source/source-id/settings", []],
  ["/workspaces/abc/destination", ["AllDestinationsPage"]],
  ["/workspaces/abc/destination/destination-id", ["DestinationItemPage", "DestinationSettingsPage"]],
  ["/workspaces/abc/destination/destination-id/connections", ["DestinationItemPage", "DestinationConnectionsPage"]],
  ["/workspaces/abc/destination/new-destination", []],
  ["/workspaces/abc/destination/enterprise/x", []],
  ["/workspaces/abc/destination/destination-id/settings", []],
  ["/workspaces/abc/connector-builder", []],
  ["/workspaces/abc/connector-builder/create", []],
  ["/workspaces/abc/connector-builder/", []],
  ["/", []],
  ["/login", []],
  ["/organization/x/settings", []],
  ["/workspaces/abc/settings/workspace", []],
  ["/workspaces", []],
];

describe("getRouteCodeChunks", () => {
  it.each(ROUTE_CODE_CASES)("%s", (pathname, expectedChunks) => {
    expect(getRouteCodeChunks(pathname)).toEqual(expectedChunks);
  });
});

describe("ROUTE_CODE_MODULES", () => {
  it("maps every route chunk to a lazy import target", () => {
    const sourceRoot = path.resolve(__dirname, "..");
    const readdirSyncRecursive = fs.readdirSync as unknown as (
      directory: string,
      options: { recursive: true }
    ) => string[];
    const sourceFiles = readdirSyncRecursive(sourceRoot, { recursive: true }).filter(
      (filePath) => /\.(?:ts|tsx)$/.test(filePath) && !/\.(?:test|spec)\.tsx?$/.test(filePath)
    );
    const lazyImportPattern = /\b(?:React\.)?lazy\s*\(\s*\(\s*\)\s*=>\s*import\s*\(\s*["']([^"']+)["']\s*\)/g;
    const lazyRouteModuleIds = new Set(
      sourceFiles.flatMap((filePath) => {
        const importingFile = path.join(sourceRoot, filePath);
        const source = fs.readFileSync(importingFile, "utf8");

        return Array.from(source.matchAll(lazyImportPattern), ([, specifier]) => {
          const modulePath = specifier.startsWith(".")
            ? path.resolve(path.dirname(importingFile), specifier)
            : path.resolve(sourceRoot, specifier);

          return path
            .relative(sourceRoot, modulePath)
            .split(path.sep)
            .join("/")
            .replace(/\.[^/.]+$/, "")
            .replace(/\/index$/, "");
        });
      })
    );

    const missingModules = Object.values(ROUTE_CODE_MODULES).filter((moduleId) => !lazyRouteModuleIds.has(moduleId));

    expect(() => {
      if (missingModules.length > 0) {
        throw new Error(`ROUTE_CODE_MODULES contains non-lazy route targets: ${missingModules.join(", ")}`);
      }
    }).not.toThrow();
  });
});

describe("preloadRouteCode", () => {
  const routeChunkData = {
    "pages/connections/ConnectionsRoutes": {
      js: ["/assets/connections.js", "/assets/shared.js"],
      css: ["/assets/connections.css", "/assets/shared.css"],
    },
    "pages/connections/AllConnectionsPage": {
      js: ["/assets/shared.js", "/assets/connections-list.js"],
      css: ["/assets/shared.css", "/assets/connections-list.css"],
    },
  };

  const addRouteChunkData = (data: typeof routeChunkData = routeChunkData) => {
    const script = document.createElement("script");
    script.id = "airbyte-route-chunks";
    script.type = "application/json";
    script.textContent = JSON.stringify(data);
    document.head.append(script);
  };

  beforeEach(() => {
    document.head.innerHTML = "";
  });

  it("adds deduplicated preload links for the matched route modules", () => {
    addRouteChunkData();

    preloadRouteCode("/workspaces/abc/connections");

    expect(
      Array.from(document.querySelectorAll('link[rel="modulepreload"]')).map((link) => link.getAttribute("href"))
    ).toEqual(["/assets/connections.js", "/assets/shared.js", "/assets/connections-list.js"]);
    expect(
      Array.from(document.querySelectorAll('link[rel="preload"][as="style"]')).map((link) => link.getAttribute("href"))
    ).toEqual(["/assets/connections.css", "/assets/shared.css", "/assets/connections-list.css"]);
    expect(document.querySelector('link[rel="modulepreload"]')?.getAttribute("crossorigin")).toBe("");
    expect(document.querySelector('link[rel="preload"][as="style"]')?.getAttribute("crossorigin")).toBe("");
  });

  it("ignores prefetch links and skips existing same-kind preload links", () => {
    addRouteChunkData();

    const prefetchLink = document.createElement("link");
    prefetchLink.rel = "prefetch";
    prefetchLink.href = "/assets/connections.js";
    document.head.append(prefetchLink);

    const modulepreloadLink = document.createElement("link");
    modulepreloadLink.rel = "modulepreload";
    modulepreloadLink.href = "/assets/shared.js";
    document.head.append(modulepreloadLink);

    const stylePreloadLink = document.createElement("link");
    stylePreloadLink.rel = "preload";
    stylePreloadLink.setAttribute("as", "style");
    stylePreloadLink.href = "/assets/connections.css";
    document.head.append(stylePreloadLink);

    const stylesheetLink = document.createElement("link");
    stylesheetLink.rel = "stylesheet";
    stylesheetLink.href = "/assets/shared.css";
    document.head.append(stylesheetLink);

    preloadRouteCode("/workspaces/abc/connections");

    expect(document.querySelectorAll('link[rel="prefetch"][href="/assets/connections.js"]')).toHaveLength(1);
    expect(document.querySelectorAll('link[rel="modulepreload"][href="/assets/connections.js"]')).toHaveLength(1);
    expect(document.querySelectorAll('link[rel="modulepreload"][href="/assets/shared.js"]')).toHaveLength(1);
    expect(document.querySelectorAll('link[rel="preload"][as="style"][href="/assets/connections.css"]')).toHaveLength(
      1
    );
    expect(document.querySelectorAll('link[rel="stylesheet"][href="/assets/shared.css"]')).toHaveLength(1);
    expect(
      document.querySelectorAll('link[rel="preload"][as="style"][href="/assets/connections-list.css"]')
    ).toHaveLength(1);
  });

  it("does nothing when the route map is absent", () => {
    preloadRouteCode("/workspaces/abc/connections");

    expect(document.querySelectorAll('link[rel="modulepreload"], link[rel="preload"]')).toHaveLength(0);
  });

  it("does not throw when the route map contains invalid JSON", () => {
    const script = document.createElement("script");
    script.id = "airbyte-route-chunks";
    script.type = "application/json";
    script.textContent = "{";
    document.head.append(script);

    expect(() => preloadRouteCode("/workspaces/abc/connections")).not.toThrow();
    expect(document.querySelectorAll('link[rel="modulepreload"], link[rel="preload"]')).toHaveLength(0);
  });
});
