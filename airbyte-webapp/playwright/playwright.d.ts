import { FeatureSet } from "@src/core/services/features/types";
import { Experiments } from "@src/hooks/services/Experiment/experiments";

declare global {
  interface Window {
    _e2eOverwrites?: Partial<Experiments>;
    _e2eFeatureOverwrites?: FeatureSet;
    // Playwright E2E environment detection
    _e2ePlaywrightEnvironment?: boolean;
  }
}

export {};
