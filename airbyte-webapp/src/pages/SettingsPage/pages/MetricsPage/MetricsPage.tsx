import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import { useCurrentWorkspace } from "core/api";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import MetricsForm from "./components/MetricsForm";
import useWorkspaceEditor from "../../components/useWorkspaceEditor";

export const MetricsPage: React.FC = () => {
  const workspace = useCurrentWorkspace();
  const { errorMessage, successMessage, loading, updateData } = useWorkspaceEditor();

  useTrackPage(PageTrackingCodes.SETTINGS_METRICS);
  const onChange = async (data: { anonymousDataCollection: boolean }) => {
    await updateData({ ...workspace, ...data, news: !!workspace.news, securityUpdates: !!workspace.securityUpdates });
  };

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.metrics" }]} />
      <Card title={<FormattedMessage id="settings.metricsSettings" />}>
        <Box p="xl">
          <MetricsForm
            onChange={onChange}
            anonymousDataCollection={workspace.anonymousDataCollection}
            successMessage={successMessage}
            errorMessage={errorMessage}
            isLoading={loading}
          />
        </Box>
      </Card>
    </>
  );
};
