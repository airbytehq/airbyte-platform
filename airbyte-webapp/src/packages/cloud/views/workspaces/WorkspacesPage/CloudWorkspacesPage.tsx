import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import styles from "./CloudWorkspacesPage.module.scss";
import { CloudWorkspacesList } from "./components/CloudWorkspacesList";
import { ReactComponent as AirbyteLogo } from "./components/workspaceHeaderLogo.svg";

export const CloudWorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);

  return (
    <div className={styles.container}>
      <AirbyteLogo className={styles.logo} width={186} />
      <Heading as="h1" size="lg" centered>
        <FormattedMessage id="workspaces.title" />
      </Heading>
      <Text align="center" className={styles.subtitle}>
        <FormattedMessage id="workspaces.subtitle" />
      </Text>
      <Box pb="2xl">
        <CloudWorkspacesList />
      </Box>
    </div>
  );
};
