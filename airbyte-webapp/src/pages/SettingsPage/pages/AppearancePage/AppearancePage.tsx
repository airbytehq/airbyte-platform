import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import { useCurrentWorkspace } from "services/workspaces/WorkspacesService";

import AppearanceMode from "./components/AppearanceMode";
import useWorkspaceEditor from "../../components/useWorkspaceEditor";
import { Content, SettingsCard } from "../SettingsComponents";

const AppearancePage: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { errorMessage, successMessage, loading, updateData } = useWorkspaceEditor();

  useTrackPage(PageTrackingCodes.SETTINGS_METRICS);
  const onChange = async (data: { anonymousDataCollection: boolean }) => {
    await updateData({ ...workspace, ...data, news: !!workspace.news, securityUpdates: !!workspace.securityUpdates });
  };

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.appearance" }]} />
      <SettingsCard title={<FormattedMessage id="settings.appearanceSettings" />}>
        <Content>
          <AppearanceMode
            onChange={onChange}
            anonymousDataCollection={workspace.anonymousDataCollection}
            successMessage={successMessage}
            errorMessage={errorMessage}
            isLoading={loading}
          />
        </Content>
      </SettingsCard>
    </>
  );
};

export default AppearancePage;
