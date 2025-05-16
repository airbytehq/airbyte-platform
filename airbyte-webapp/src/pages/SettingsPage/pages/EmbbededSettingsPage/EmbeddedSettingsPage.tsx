import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Text } from "components/ui/Text/Text";

import { useCurrentWorkspace, useListApplications } from "core/api";

import EmbeddedLogo from "./embedded-logo.svg?react";
import styles from "./EmbeddedSettingsPage.module.scss";

export const EmbeddedSettingsPage: React.FC = () => {
  const { organizationId } = useCurrentWorkspace();
  const { applications } = useListApplications();

  const envContent = `AIRBYTE_ORGANIZATION_ID=${organizationId}
AIRBYTE_CLIENT_ID=${applications[0]?.clientId}
AIRBYTE_CLIENT_SECRET=${applications[0]?.clientSecret}`;

  return (
    <FlexContainer direction="column">
      <Text>
        <EmbeddedLogo />
      </Text>
      <Box pb="md">
        <Text color="grey400">
          <FormattedMessage id="embedded.envDescription" />
        </Text>

        <Box py="md">
          <ul className={styles.list}>
            <li>
              <Box py="md">
                <Text bold size="lg" as="span">
                  <FormattedMessage id="embedded.envVariables.organizationId" />
                </Text>
                <Text size="lg" as="span">
                  {organizationId}
                </Text>
              </Box>
            </li>
            <li>
              <Box py="md">
                <Text bold size="lg" as="span">
                  <FormattedMessage id="embedded.envVariables.clientId" />
                </Text>
                <Text size="lg" as="span">
                  {applications[0]?.clientId}
                </Text>
              </Box>
            </li>
            <li>
              <Box py="md">
                <Text size="lg" as="span" bold>
                  <FormattedMessage id="embedded.envVariables.clientSecret" />
                </Text>
                <Text size="lg" as="span">
                  {applications[0]?.clientSecret}
                </Text>
              </Box>
            </li>
          </ul>
        </Box>
        <CopyButton content={envContent}>
          <FormattedMessage id="copyButton.title" />
        </CopyButton>
      </Box>
    </FlexContainer>
  );
};
