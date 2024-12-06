import React from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { isCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { CloudHelpDropdown } from "packages/cloud/views/layout/CloudMainView/CloudHelpDropdown";
import { RoutePaths } from "pages/routePaths";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";
import { HelpDropdown } from "views/layout/SideBar/components/HelpDropdown";

import { DownloadYamlButton } from "./DownloadYamlButton";
import styles from "./MenuBar.module.scss";
import { PublishButton } from "./PublishButton";
import AssistConfigButton from "../Builder/Assist/AssistConfigButton";
import { HotkeyLabel, getCtrlOrCmdKey } from "../HotkeyLabel";
import { NameInput } from "../NameInput";
import { useBuilderWatch } from "../types";

export const MenuBar: React.FC = () => {
  const {
    undoRedo: { canUndo, canRedo, undo, redo },
  } = useConnectorBuilderFormState();
  const mode = useBuilderWatch("mode");

  useHotkeys(
    ["ctrl+z", "meta+z"],
    (event) => {
      event.preventDefault();
      undo();
    },
    { enableOnFormTags: ["input", "textarea", "select"] }
  );

  useHotkeys(
    ["ctrl+shift+z", "meta+shift+z"],
    (event) => {
      event.preventDefault();
      redo();
    },
    { enableOnFormTags: ["input", "textarea", "select"] }
  );

  return (
    <FlexContainer
      direction="row"
      alignItems="center"
      justifyContent="space-between"
      gap="xl"
      className={styles.container}
    >
      <FlexContainer direction="row" alignItems="center" gap="lg">
        <FlexContainer direction="row" alignItems="center" gap="md" className={styles.exitAndName}>
          <Link to={RoutePaths.ConnectorBuilder}>
            <Button
              variant="clearDark"
              size="xs"
              icon="arrowLeft"
              iconSize="lg"
              type="button"
              data-testid="exit-builder"
            >
              <Text className={styles.backButtonText}>
                <FormattedMessage id="connectorBuilder.exit" />
              </Text>
            </Button>
          </Link>
          <Text className={styles.separator}>/</Text>
          <FlexContainer className={styles.nameContainer}>
            <NameInput className={styles.connectorName} size="sm" showBorder />
          </FlexContainer>
        </FlexContainer>
        {mode === "ui" && (
          <FlexContainer direction="row" alignItems="center" gap="sm">
            <Tooltip
              control={
                <Button
                  className={styles.undoButton}
                  variant="clearDark"
                  size="xs"
                  icon="rotate"
                  iconSize="md"
                  type="button"
                  disabled={!canUndo}
                  onClick={undo}
                />
              }
            >
              <FlexContainer direction="column" gap="md" alignItems="center">
                <FormattedMessage id="connectorBuilder.undo" />
                <HotkeyLabel keys={[getCtrlOrCmdKey(), "Z"]} />
              </FlexContainer>
            </Tooltip>
            <Tooltip
              control={
                <Button
                  variant="clearDark"
                  size="xs"
                  icon="rotate"
                  iconSize="md"
                  type="button"
                  disabled={!canRedo}
                  onClick={redo}
                />
              }
            >
              <FlexContainer direction="column" gap="md" alignItems="center">
                <FormattedMessage id="connectorBuilder.redo" />
                <HotkeyLabel keys={[getCtrlOrCmdKey(), "Shift", "Z"]} />
              </FlexContainer>
            </Tooltip>
          </FlexContainer>
        )}
      </FlexContainer>
      <FlexContainer direction="row" alignItems="center">
        <AssistConfigButton />
      </FlexContainer>
      <FlexContainer direction="row" alignItems="center" className={styles.rightSide}>
        {isCloudApp() ? (
          <CloudHelpDropdown className={styles.helpButton} hideLabel placement="bottom" />
        ) : (
          <HelpDropdown className={styles.helpButton} hideLabel placement="bottom" />
        )}
        <Tooltip
          placement="bottom"
          control={
            <a href="https://airbytehq.slack.com/archives/C027KKE4BCZ" target="_blank" rel="noreferrer">
              <Button variant="clearDark" size="xs" icon="slack" iconSize="md" type="button" />
            </a>
          }
        >
          <FormattedMessage id="connectorBuilder.slackChannelTooltip" />
        </Tooltip>
        <Tooltip
          placement="bottom"
          control={
            <a href={links.connectorBuilderTutorial} target="_blank" rel="noreferrer">
              <Button variant="clearDark" size="xs" icon="docs" iconSize="md" type="button" />
            </a>
          }
        >
          <FormattedMessage id="connectorBuilder.tutorialTooltip" />
        </Tooltip>
        <DownloadYamlButton />
        <PublishButton />
      </FlexContainer>
    </FlexContainer>
  );
};
