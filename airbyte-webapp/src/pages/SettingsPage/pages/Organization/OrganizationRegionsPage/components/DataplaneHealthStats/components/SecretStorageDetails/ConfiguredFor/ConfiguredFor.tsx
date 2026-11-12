import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";

import { ScopeName } from "./ScopeName";
import { StoreScope, StoreScopeType } from "../types";

const SCOPE_MESSAGE_ID: Record<StoreScopeType, string> = {
  workspace: "settings.organization.regions.dataplane.diagnostics.secretStorage.details.scope.workspace",
  organization: "settings.organization.regions.dataplane.diagnostics.secretStorage.details.scope.organization",
};

interface ConfiguredForProps {
  scope: StoreScope;
}

// A flex row, so the spinner that stands in for the name sits next to the text.
export const ConfiguredFor: React.FC<ConfiguredForProps> = ({ scope }) => (
  <FlexContainer alignItems="center" gap="xs">
    <FormattedMessage
      id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.scopeWithName"
      values={{ scope: <FormattedMessage id={SCOPE_MESSAGE_ID[scope.type]} />, name: <ScopeName scope={scope} /> }}
    />
  </FlexContainer>
);
