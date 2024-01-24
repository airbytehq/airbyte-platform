import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { MetricsForm } from "./components/MetricsForm";

export const MetricsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_METRICS);

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.metrics" }]} />
      <Card title={<FormattedMessage id="settings.metricsSettings" />}>
        <Box p="xl">
          <MetricsForm />
        </Box>
      </Card>
    </>
  );
};
