import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { MetricsForm } from "./components/MetricsForm";

export const MetricsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  useTrackPage(PageTrackingCodes.SETTINGS_METRICS);

  return (
    <FlexContainer direction="column" gap="xl">
      <Heading as="h1">{formatMessage({ id: "settings.metricsSettings" })}</Heading>
      <MetricsForm />
    </FlexContainer>
  );
};
