import type { ConnectionHandle } from "./builders/connection";
import type { Experiments } from "@src/core/services/Experiment/experiments";
import type { FeatureSet } from "@src/core/services/features/types";

/**
 * The backend state a test starts from. Every test builds a fresh store from its scenario, so a scenario is
 * safe to share between tests through `test.use({ airbyteScenario })`.
 *
 * The identity is fixed: OSS community edition, auth mode "none", one organization, one workspace,
 * and one user with organization_admin permission.
 */
export interface ScenarioInput {
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
