import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate } from "react-router-dom";

import { HeadTitle } from "components/HeadTitle";
import { PageViewContainer } from "components/PageViewContainer";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";

import styles from "./OnboardingPage.module.scss";
import { OnboardingSurvey } from "./OnboardingSurvey";

export const OnboardingPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.ONBOARDING);
  const isSurveyEnabled = useExperiment("onboarding.surveyEnabled");

  // If survey is not enabled, redirect to connections page
  if (!isSurveyEnabled) {
    return <Navigate to={RoutePaths.Connections} replace />;
  }

  return (
    <PageViewContainer>
      <HeadTitle titles={[{ id: "onboarding.title" }]} />
      <FlexContainer direction="column" gap="xl" alignItems="center">
        <Heading as="h1" size="lg">
          <FormattedMessage id="onboarding.title" />
        </Heading>
        <Box className={styles.content}>
          <Text size="lg" align="center">
            <FormattedMessage id="onboarding.description" />
          </Text>
        </Box>

        <OnboardingSurvey />
      </FlexContainer>
    </PageViewContainer>
  );
};
