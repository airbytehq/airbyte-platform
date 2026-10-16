import type { ConnectionHandle } from "./builders/connection";
import type {
  DestinationDefinitionHandle,
  DestinationHandle,
  SourceDefinitionHandle,
  SourceHandle,
} from "./builders/connectors";
import type { CheckOutput } from "./commands";
import type { SecurityCheckAnswer } from "./external";
import type { SyncOutcome } from "./jobs";
import type { UserRead, ConnectorBuilderProjectStreamRead } from "@src/core/api/types/AirbyteClient";
import type { Experiments } from "@src/core/services/Experiment/experiments";
import type { FeatureSet } from "@src/core/services/features/types";

/**
 * The backend state a test starts from. Every test builds a fresh store from its scenario, so a scenario is
 * safe to share between tests through `test.use({ airbyteScenario })`.
 *
 * The identity is OSS community edition, auth mode "none", one organization, one workspace, and one user with
 * organization_admin permission. `instance` and `user` change parts of it.
 */
export interface ScenarioInput {
  /** `initialSetupComplete: false` starts on an instance where nobody has submitted the setup page yet. Default: true. */
  instance?: { initialSetupComplete?: boolean };
  /** User fields that differ from the default user, such as `email` or `metadata`. The user ID does not change. */
  user?: Partial<Omit<UserRead, "userId">>;
  /** Answers of hosts outside Airbyte that the webapp calls. external.ts lists the hosts. */
  external?: { securityCheck?: SecurityCheckAnswer };
  /** Source definitions in the marketplace. The definitions of the sources in `connections` are added automatically. */
  sourceDefinitions?: SourceDefinitionHandle[];
  /** Sources without a connection. The sources in `connections` are added automatically. */
  sources?: SourceHandle[];
  /** Destination definitions in the marketplace. The definitions of the destinations in `connections` are added automatically. */
  destinationDefinitions?: DestinationDefinitionHandle[];
  /** Destinations without a connection. The destinations in `connections` are added automatically. */
  destinations?: DestinationHandle[];
  /** Sources and destinations that a connection references are added automatically. */
  connections?: ConnectionHandle[];
  /** Experiment overrides, sent to the webapp as window._e2eOverwrites. */
  experiments?: Partial<Experiments>;
  /** Feature overrides, sent to the webapp as window._e2eFeatureOverwrites. */
  features?: FeatureSet;
  /** Results of long-running operations that finish on their own. See lifecycle.ts. */
  outcomes?: {
    /** Default: the check succeeds. */
    check?: CheckOutput;
    /** Default: the sync succeeds. */
    sync?: SyncOutcome;
    /** The result of every Connector Builder test read. No default: a test read without it fails the test. */
    streamRead?: ConnectorBuilderProjectStreamRead;
  };
}

export interface Scenario {
  readonly input: Readonly<ScenarioInput>;
  /** Shallow merge of top-level keys. Arrays replace, they do not concatenate. */
  with(patch: ScenarioInput): Scenario;
}

export function scenario(input: ScenarioInput): Scenario {
  return Object.freeze({
    input: Object.freeze({ ...input }),
    with: (patch: ScenarioInput) => scenario({ ...input, ...patch }),
  });
}
