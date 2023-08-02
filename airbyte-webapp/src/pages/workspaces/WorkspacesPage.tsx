import React from "react";
import { FormattedMessage } from "react-intl";

import { HeadTitle } from "components/common/HeadTitle";
import { ReactComponent as AirbyteLogo } from "components/illustrations/airbyte-logo.svg";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { NextPageHeader } from "components/ui/PageHeader";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import { useTrackPage, PageTrackingCodes } from "core/services/analytics";

import WorkspacesList from "./components/WorkspacesList";
import styles from "./WorkspacesPage.module.scss";

const WorkspacesPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.WORKSPACES);

  return (
    <>
      <HeadTitle titles={[{ id: "workspaces.title" }]} />
      <Box px="lg" className={styles.brandingHeader}>
        <AirbyteLogo width={110} />
      </Box>
      <NextPageHeader
        leftComponent={
          <FlexContainer direction="column" alignItems="flex-start" justifyContent="flex-start">
            <FlexContainer direction="row" gap="none">
              <Heading as="h1" size="md">
                <FormattedMessage id="workspaces.title" />
              </Heading>
              <InfoTooltip>
                <Text inverseColor>
                  <FormattedMessage id="workspaces.subtitle" />
                </Text>
              </InfoTooltip>
            </FlexContainer>
          </FlexContainer>
        }
      />
      <Box py="2xl" className={styles.content}>
        <WorkspacesList />
      </Box>
    </>
  );
};

export default WorkspacesPage;
