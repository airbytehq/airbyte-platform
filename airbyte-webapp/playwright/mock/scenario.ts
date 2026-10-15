import type { ConnectionHandle } from "./builders/connection";
import type { SecurityCheckAnswer } from "./external";
import type { UserRead } from "@src/core/api/types/AirbyteClient";
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
  /** Sources and destinations that a connection references are added automatically. */
  connections?: ConnectionHandle[];
  /** Experiment overrides, sent to the webapp as window._e2eOverwrites. */
  experiments?: Partial<Experiments>;
  /** Feature overrides, sent to the webapp as window._e2eFeatureOverwrites. */
  features?: FeatureSet;
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
