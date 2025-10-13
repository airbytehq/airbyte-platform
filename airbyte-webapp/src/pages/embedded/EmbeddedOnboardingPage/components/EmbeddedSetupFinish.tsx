import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text/Text";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useUpdateEmbeddedOnboardingStatus } from "core/api";
import { OnboardingStatusEnum } from "core/api/types/SonarClient";
import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./EmbeddedSetupFinish.module.scss";

export const EmbeddedSetupFinish: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_FINISH);
  const { mutate: updateOrganizationOnboardingProgress } = useUpdateEmbeddedOnboardingStatus();
  const navigate = useNavigate();
  const organizationId = useCurrentOrganizationId();

  const onClickFinish = () => {
    updateOrganizationOnboardingProgress({ organizationId, status: OnboardingStatusEnum.COMPLETED });
    navigate("/"); // Redirect to the main view after finishing
  };
  return (
    <Box py="2xl" className={styles.container}>
      <FlexContainer direction="column" gap="lg">
        <Text as="h2" size="xl" bold>
          <FormattedMessage id="embedded.onboarding.finish.allSet" />
        </Text>
        <Text>
          <FormattedMessage id="embedded.onboarding.finish.description" />
        </Text>
        <Button full data-testid="embedded-onboarding-finish-cta" onClick={onClickFinish}>
          <FormattedMessage id="embedded.onboarding.finish.cta" />
        </Button>
      </FlexContainer>
    </Box>
  );
};
