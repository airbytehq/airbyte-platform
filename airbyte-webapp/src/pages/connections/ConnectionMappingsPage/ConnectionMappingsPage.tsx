import { FormattedMessage, useIntl } from "react-intl";
import { Navigate } from "react-router-dom";

import { FormChangeTracker } from "components/forms/FormChangeTracker";
import { PageContainer } from "components/PageContainer";
import { BrandingBadge } from "components/ui/BrandingBadge";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { ScrollParent } from "components/ui/ScrollParent";

import { useIsDataActivationConnection } from "area/connection/utils/useIsDataActivationConnection";
import { FeatureItem, IfFeatureDisabled, IfFeatureEnabled } from "core/services/features";
import { useFormMode } from "core/services/ui/FormModeContext";
import { links } from "core/utils/links";
import { useOrganizationSubscriptionStatus } from "core/utils/useOrganizationSubscriptionStatus";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useNotificationService } from "hooks/services/Notification";
import { ConnectionRoutePaths } from "pages/routePaths";

import { ConnectionMappingsList } from "./ConnectionMappingsList";
import styles from "./ConnectionMappingsPage.module.scss";
import { MappingContextProvider, useMappingContext, MAPPING_VALIDATION_ERROR_KEY } from "./MappingContext";
import { MappingsEmptyState } from "./MappingsEmptyState";
import { MappingsUpsellEmptyState } from "./MappingsUpsellEmptyState";

export const ConnectionMappingsRoute = () => {
  const isDataActivationConnection = useIsDataActivationConnection();

  // Should only happen if someone tries to access the /mappings URL directly for a data activation connection
  if (isDataActivationConnection) {
    return <Navigate to={`../${ConnectionRoutePaths.DataActivationMappings}`} replace />;
  }

  return (
    <ScrollParent>
      <PageContainer centered>
        <MappingContextProvider>
          <ConnectionMappingsPage />
        </MappingContextProvider>
      </PageContainer>
    </ScrollParent>
  );
};

const ConnectionMappingsPage = () => {
  const { isUnifiedTrialPlan } = useOrganizationSubscriptionStatus();
  const { streamsWithMappings, clear, submitMappings, hasMappingsChanged } = useMappingContext();
  const { mode } = useFormMode();
  const { connectionUpdating } = useConnectionEditService();
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const handleValidations = async () => {
    const validations = await Promise.allSettled(
      Object.entries(streamsWithMappings).flatMap(([_streamName, mappers]) =>
        mappers.map((mapper) => mapper.validationCallback())
      )
    );

    const hasServerValidationErrors = Object.entries(streamsWithMappings).some(([_, mappings]) => {
      return mappings.some((mapper) => !!mapper.validationError);
    });

    if (
      validations.every((validation) => validation.status === "fulfilled" && validation.value === true) &&
      !hasServerValidationErrors
    ) {
      await submitMappings();
    } else {
      registerNotification({
        type: "error",
        text: formatMessage({ id: "connections.mappings.submissionValidationError" }),
        id: MAPPING_VALIDATION_ERROR_KEY,
      });
    }
  };

  const anyMappersConfigured = Object.entries(streamsWithMappings).some(([_streamName, mappers]) => mappers.length > 0);
  const streamsWithEmptyMappersExist = Object.entries(streamsWithMappings).some(
    ([_streamName, mappers]) => mappers.length === 0
  );
  const showSubmissionButtons = anyMappersConfigured || streamsWithEmptyMappersExist;

  return (
    <>
      <IfFeatureEnabled feature={FeatureItem.MappingsUI}>
        <FlexContainer direction="column">
          <FlexContainer
            direction="row"
            justifyContent="space-between"
            alignItems="center"
            className={styles.pageTitleContainer}
          >
            <FlexItem grow>
              <FlexContainer direction="row" alignItems="center" gap="md">
                <Heading as="h3" size="sm">
                  <FormattedMessage id="connections.mappings.title" />
                </Heading>
                {isUnifiedTrialPlan && (
                  <BrandingBadge product="cloudForTeams" testId="cloud-for-teams-badge-mappings" />
                )}
              </FlexContainer>
            </FlexItem>
            <ExternalLink href={links.connectionMappings}>
              <Button variant="clear" icon="share" iconPosition="right" iconSize="sm">
                <FormattedMessage id="connections.mappings.docsLink" />
              </Button>
            </ExternalLink>
            <FormChangeTracker formId="mapping-form" changed={hasMappingsChanged} />
            {showSubmissionButtons && (
              <>
                <Button
                  variant="secondary"
                  onClick={clear}
                  disabled={mode === "readonly" || !hasMappingsChanged || connectionUpdating}
                >
                  <FormattedMessage id="form.cancel" />
                </Button>
                <Button
                  isLoading={connectionUpdating}
                  onClick={handleValidations}
                  disabled={mode === "readonly" || !hasMappingsChanged}
                  data-testid="submit-mappings"
                >
                  <FormattedMessage id="form.submit" />
                </Button>
              </>
            )}
          </FlexContainer>
          {anyMappersConfigured ? <ConnectionMappingsList /> : <MappingsEmptyState />}
        </FlexContainer>
      </IfFeatureEnabled>
      <IfFeatureDisabled feature={FeatureItem.MappingsUI}>
        <MappingsUpsellEmptyState />
      </IfFeatureDisabled>
    </>
  );
};
