import React from "react";
import { FormattedMessage } from "react-intl";

import { ReactComponent as AirbyteLogo } from "components/illustrations/airbyte-logo.svg";
import { Box } from "components/ui/Box";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { useCreateCloudWorkspace, useListCloudWorkspaces } from "core/api/cloud";
import { useTrackPage, PageTrackingCodes } from "core/services/analytics";
import WorkspacesList from "pages/workspaces/components/WorkspacesList";

import styles from "./CloudWorkspacesPage.module.scss";

export const CloudWorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);
  const { workspaces } = useListCloudWorkspaces();
  const { mutateAsync: createWorkspace } = useCreateCloudWorkspace();

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
        <WorkspacesList workspaces={workspaces} createWorkspace={createWorkspace} />
      </Box>
    </div>
  );
};
