import { useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useSearchParams } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { DESTINATION_DEFINITION_PARAM } from "components/connection/CreateConnection/CreateNewDestination";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useListApplications, useUpdateEmbeddedOnboardingStatus } from "core/api";
import { OnboardingStatusEnum } from "core/api/types/SonarClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";
import { links } from "core/utils/links";

import styles from "./EmbedCodeStep.module.scss";
import { EMBEDDED_ONBOARDING_STEP_PARAM, EmbeddedOnboardingStep } from "../EmbeddedOnboardingPageLayout";

export const EmbedCodeStep: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_EMBED_CODE);
  const [searchParams, setSearchParams] = useSearchParams();

  const organizationId = useCurrentOrganizationId();
  const { applications } = useListApplications();
  const { mutate: updateOrganizationOnboardingProgress } = useUpdateEmbeddedOnboardingStatus();

  const envContent = useMemo(() => {
    if (!organizationId || !applications[0]?.clientId || !applications[0]?.clientSecret) {
      return "Missing application credentials. Please contact support.";
    }
    return `AIRBYTE_ORGANIZATION_ID=${organizationId}
AIRBYTE_CLIENT_ID=${applications[0]?.clientId}
AIRBYTE_CLIENT_SECRET=${applications[0]?.clientSecret}`;
  }, [organizationId, applications]);

  useEffectOnce(() => searchParams.delete(DESTINATION_DEFINITION_PARAM));

  const onClickNext = () => {
    updateOrganizationOnboardingProgress({ organizationId, status: OnboardingStatusEnum.EMBED_CODE_COPIED });
    setSearchParams({ [EMBEDDED_ONBOARDING_STEP_PARAM]: EmbeddedOnboardingStep.Finish });
  };

  const onClickBack = () => {
    setSearchParams({ [EMBEDDED_ONBOARDING_STEP_PARAM]: EmbeddedOnboardingStep.SelectDestination });
  };

  return (
    <Box py="2xl" mt="2xl" className={styles.content}>
      <FlexContainer direction="column" gap="xl">
        <Text as="h2" size="xl" bold>
          <FormattedMessage id="embedded.onboarding.embedCodeTitle" />
        </Text>
        <Text color="grey400">
          <FormattedMessage id="embedded.onboarding.embedCode.description" />
        </Text>

        <Card>
          <Box>
            <Text size="md" className={styles.envContent}>
              <Pre>{envContent}</Pre>
            </Text>
          </Box>
          <FlexContainer justifyContent="space-between">
            <ExternalLink variant="button" href={links.embeddedOnboardingDocs} className={styles.viewDocsLink}>
              <Icon type="export" />
              <FormattedMessage id="embedded.viewDocs" defaultMessage="View docs" />
            </ExternalLink>
            <CopyButton full content={envContent}>
              <FormattedMessage id="embedded.onboarding.embedCode.copy" />
            </CopyButton>
          </FlexContainer>
        </Card>

        <FlexContainer justifyContent="space-between">
          <Button variant="secondary" onClick={onClickBack}>
            <FormattedMessage id="embedded.onboarding.backToSetup" />
          </Button>
          <Button variant="primary" onClick={onClickNext} data-testid="copy-embed-code-next">
            <FormattedMessage id="embedded.onboarding.next" />
          </Button>
        </FlexContainer>
      </FlexContainer>
    </Box>
  );
};
