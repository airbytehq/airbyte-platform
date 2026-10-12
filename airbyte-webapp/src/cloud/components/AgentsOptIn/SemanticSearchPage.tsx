import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { LoadingPage } from "components/ui/LoadingPage";
import { Text } from "components/ui/Text";

import { useOrganizationSemanticSearchEnablement } from "core/api";

import { ContextLayerLoadError, ContextLayerSettingsEmptyState, ContextLayerUnavailable } from "./ContextLayerPage";
import styles from "./SemanticSearchPage.module.scss";
import { useContextLayerEligibility } from "./useContextLayerEligibility";

const SemanticSearchSettingSection: React.FC = () => {
  const semanticSearchQuery = useOrganizationSemanticSearchEnablement();

  if (semanticSearchQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (semanticSearchQuery.isError) {
    return <ContextLayerLoadError onRetry={() => semanticSearchQuery.refetch()} />;
  }
  if (!semanticSearchQuery.data) {
    return <LoadingPage />;
  }

  const semanticSearchEnabled = semanticSearchQuery.data.semanticSearchEnabled;
  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="lg">
        <FlexContainer direction="column" gap="md">
          <Heading as="h2" size="lg">
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.title" />
          </Heading>
          <Text>
            <FormattedMessage id="cloud.contextLayer.semanticSearchPage.description" />
          </Text>
        </FlexContainer>
        {!semanticSearchEnabled && (
          <ContextLayerSettingsEmptyState
            title={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.disabled.title" />}
            description={<FormattedMessage id="cloud.contextLayer.semanticSearchPage.disabled.description" />}
          />
        )}
      </FlexContainer>
    </div>
  );
};

const SemanticSearchPageContent: React.FC = () => {
  const { isCloudApp, statusQuery, status, isEligible } = useContextLayerEligibility();

  if (isCloudApp && statusQuery.isInitialLoading) {
    return <LoadingPage />;
  }
  if (isCloudApp && statusQuery.isError) {
    return <ContextLayerLoadError onRetry={() => statusQuery.refetch()} />;
  }
  if (!isCloudApp || !isEligible || !status) {
    return <ContextLayerUnavailable />;
  }

  return <SemanticSearchSettingSection />;
};

export const SemanticSearchPage: React.FC = () => {
  return (
    <React.Suspense fallback={<LoadingPage />}>
      <SemanticSearchPageContent />
    </React.Suspense>
  );
};
