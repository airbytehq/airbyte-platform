import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import { ReactComponent as AirbyteLogo } from "./components/workspaceHeaderLogo.svg";
import { WorkspacesCreateControl } from "./components/WorkspacesCreateControl";
import WorkspacesList from "./components/WorkspacesList";
import styles from "./WorkspacesPage.module.scss";

const WorkspacesPage: React.FC = () => {
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
      <WorkspacesCreateControl />
      <Box pb="2xl">
        <WorkspacesList />
      </Box>
    </div>
  );
};

export default WorkspacesPage;
