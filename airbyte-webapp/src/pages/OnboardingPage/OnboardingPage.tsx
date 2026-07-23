import React from "react";
import { FormattedMessage } from "react-intl";
import { Navigate } from "react-router-dom";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { HeadTitle } from "components/ui/HeadTitle";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { useExperiment } from "core/services/Experiment";
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
    <>
      <HeadTitle titles={[{ id: "onboarding.title" }]} />
      <main className={styles.onboardingPage}>
        <Card className={styles.onboardingCard}>
          <FlexContainer direction="column" gap="xl" alignItems="center">
            <Heading as="h1" size="lg">
              <FormattedMessage id="onboarding.title" />
            </Heading>
            <OnboardingSurvey />
          </FlexContainer>
        </Card>
      </main>
    </>
  );
};
