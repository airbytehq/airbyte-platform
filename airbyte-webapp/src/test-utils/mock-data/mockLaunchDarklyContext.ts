import { LDSingleKindContext } from "launchdarkly-js-client-sdk";

export const mockUserContext: LDSingleKindContext = {
  kind: "user",
  key: "123",
};

export const mockWorkspaceContext: LDSingleKindContext = {
  kind: "workspace",
  key: "456",
};

export const mockSourceDefinitionContext: LDSingleKindContext = {
  kind: "sourceDefinition",
  key: "789",
};
