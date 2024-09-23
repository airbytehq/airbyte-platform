import { FeatureSet } from "@src/core/services/features/types";

declare namespace Cypress {
  interface AUTWindow {
    document: Document;
    navigator: Navigator;
    _e2eOverwrites?: Partial<Experiments>;
    _e2eFeatureOverwrites?: FeatureSet;
  }
}
