import type {
  ConnectorBuilderProjectDetails,
  ConnectorBuilderProjectDetailsRead,
  ConnectorBuilderProjectRead,
  ConnectorBuilderProjectStreamRead,
  ConnectorBuilderProjectTestingValues,
} from "@src/core/api/types/AirbyteClient";

import { ids } from "./ids";
import { snapshot } from "./store";

interface StoredProject {
  builderProjectId: string;
  details: ConnectorBuilderProjectDetails;
  testingValues?: ConnectorBuilderProjectTestingValues;
  updatedAt: number;
}

/**
 * Connector Builder projects that the webapp created during a test. The builder saves the draft manifest of a
 * project on every change, and saves the testing values (such as an API key) separately. A read returns both.
 *
 * `streamRead` is the result of every test read, from the scenario (`outcomes.streamRead`).
 */
export function createBuilderProjects(nowSeconds: () => number, streamRead?: ConnectorBuilderProjectStreamRead) {
  const projects: StoredProject[] = [];

  function find(builderProjectId: string) {
    return projects.find((project) => project.builderProjectId === builderProjectId);
  }

  function detailsRead({ builderProjectId, details, updatedAt }: StoredProject): ConnectorBuilderProjectDetailsRead {
    return {
      builderProjectId,
      name: details.name,
      hasDraft: details.draftManifest !== undefined,
      updatedAt,
      componentsFileContent: details.componentsFileContent,
    };
  }

  return {
    create: (details: ConnectorBuilderProjectDetails) => {
      const project = {
        builderProjectId: ids.builderProject(projects.length + 1),
        details: structuredClone(details),
        updatedAt: nowSeconds(),
      };
      projects.push(project);
      return project.builderProjectId;
    },
    /** Saves the details the webapp sent. Returns false when the project does not exist. */
    update: (builderProjectId: string, details: ConnectorBuilderProjectDetails) => {
      const project = find(builderProjectId);
      if (project) {
        project.details = structuredClone(details);
        project.updatedAt = nowSeconds();
      }
      return !!project;
    },
    setTestingValues: (builderProjectId: string, testingValues: ConnectorBuilderProjectTestingValues) => {
      const project = find(builderProjectId);
      if (project) {
        project.testingValues = structuredClone(testingValues);
      }
      return project && snapshot(testingValues);
    },
    /** The project with its draft manifest, as the builder loads it. */
    get: (builderProjectId: string): Readonly<ConnectorBuilderProjectRead> | undefined => {
      const project = find(builderProjectId);
      return (
        project &&
        snapshot({
          builderProject: detailsRead(project),
          declarativeManifest: project.details.draftManifest && {
            manifest: project.details.draftManifest,
            isDraft: true,
          },
          testingValues: project.testingValues,
        })
      );
    },
    all: () => projects.map((project) => snapshot(detailsRead(project))),
    /** The result of a test read. Throws when the scenario declares none. */
    streamRead: () => {
      if (!streamRead) {
        throw new Error(
          "The scenario declares no test read result. Set outcomes.streamRead, such as streamRead.grid()"
        );
      }
      return snapshot(streamRead);
    },
  };
}

export type BuilderProjects = ReturnType<typeof createBuilderProjects>;
