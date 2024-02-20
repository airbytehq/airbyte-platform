import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { links } from "core/utils/links";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { ConnectorImage } from "./ConnectorImage";
import { DownloadYamlButton } from "./DownloadYamlButton";
import { NameInput } from "./NameInput";
import { PublishButton } from "./PublishButton";
import { SavingIndicator } from "./SavingIndicator";
import styles from "./Sidebar.module.scss";
import SlackIcon from "./slack-icon.svg?react";
import { useBuilderWatch } from "./types";
import { UiYamlToggleButton } from "./UiYamlToggleButton";

interface SidebarProps {
  className?: string;
  yamlSelected: boolean;
}

export const Sidebar: React.FC<React.PropsWithChildren<SidebarProps>> = ({ className, yamlSelected, children }) => {
  const { toggleUI } = useConnectorBuilderFormState();
  const formValues = useBuilderWatch("formValues");
  const showSavingIndicator = yamlSelected || formValues.streams.length > 0;
  const OnUiToggleClick = () => toggleUI(yamlSelected ? "ui" : "yaml");

  return (
    <FlexContainer direction="column" alignItems="stretch" gap="xl" className={classnames(className, styles.container)}>
      <UiYamlToggleButton yamlSelected={yamlSelected} onClick={OnUiToggleClick} />

      <FlexContainer direction="column" alignItems="center">
        <ConnectorImage />

        <div className={styles.connectorName}>
          <Heading as="h2" size="sm">
            <NameInput />
          </Heading>
        </div>

        {showSavingIndicator && <SavingIndicator />}
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="xl" className={styles.modeSpecificContent}>
        {children}
      </FlexContainer>

      <FlexContainer direction="column" alignItems="stretch" gap="md">
        <DownloadYamlButton />
        <PublishButton />
      </FlexContainer>
      <FlexContainer direction="column" gap="lg">
        <Tooltip
          placement="top"
          control={
            <Text size="sm" className={styles.slackLink}>
              <a href="https://airbytehq.slack.com/archives/C027KKE4BCZ" target="_blank" rel="noreferrer">
                <FlexContainer gap="sm" justifyContent="center" alignItems="center" as="span">
                  <SlackIcon className={styles.slackIcon} />
                  <FormattedMessage id="connectorBuilder.slackChannel" />
                </FlexContainer>
              </a>
            </Text>
          }
        >
          <FormattedMessage id="connectorBuilder.slackChannelTooltip" />
        </Tooltip>
        <Text size="sm" className={styles.slackLink}>
          <a href={links.connectorBuilderTutorial} target="_blank" rel="noreferrer">
            <FlexContainer gap="sm" justifyContent="center" alignItems="center" as="span">
              <Icon type="docs" />
              <FormattedMessage id="connectorBuilder.createPage.tutorialPrompt" />
            </FlexContainer>
          </a>
        </Text>
      </FlexContainer>
    </FlexContainer>
  );
};
