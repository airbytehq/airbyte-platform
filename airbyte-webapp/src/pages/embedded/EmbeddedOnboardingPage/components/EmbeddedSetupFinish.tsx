import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import styles from "./EmbeddedSetupFinish.module.scss";

export const EmbeddedSetupFinish: React.FC = () => {
  useTrackPage(PageTrackingCodes.EMBEDDED_ONBOARDING_FINISH);

  return (
    <Box py="2xl" className={styles.container}>
      <FlexContainer direction="column" gap="lg">
        <Text as="h2" size="xl" bold>
          <FormattedMessage id="embedded.onboarding.finish.allSet" />
        </Text>
        <Text>
          <FormattedMessage id="embedded.onboarding.finish.description" />
        </Text>
        <Link variant="buttonPrimary" to="/" className={styles.button} data-testid="embedded-onboarding-finish-cta">
          <FormattedMessage id="embedded.onboarding.finish.cta" />
        </Link>
      </FlexContainer>
    </Box>
  );
};
