import React from "react";

import { FlexContainer } from "components/ui/Flex";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { GeneralSettingsSection, DeleteWorkspaceSection } from "./components";

export const WorkspaceSettingsView: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_WORKSPACE);

  return (
    <FlexContainer direction="column">
      <GeneralSettingsSection />
      <DeleteWorkspaceSection />
    </FlexContainer>
  );
};
