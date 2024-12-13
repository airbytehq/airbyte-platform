import React, { useEffect } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { isAnyModalOpen, isElementInModal } from "components/ui/Modal";
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

  // The browser default undo/redo behavior maintains a stack of all edited fields, and
  // as the user continues to undo/redo, it will continue to apply it to the next input
  // in the stack, regardless of whether the input is in a modal or not.
  // This is problematic because while we want the default browser undo/redo behavior for
  // inputs in modals, we don't want the browser undo/redo to bubble up to the non-modal
  // inputs.
  // This solves the issue by listening for the browser undo/redo event, and checking if
  // the target input is inside of a modal. We allow the default behavior if so, and
  // prevent the default behavior if not.
  useEffect(() => {
    window.addEventListener("beforeinput", (event) => {
      if (
        (event.inputType === "historyUndo" || event.inputType === "historyRedo") &&
        event.target instanceof Element &&
        !isElementInModal(event.target)
      ) {
        event.preventDefault();
      }
    });
  }, []);

  useHotkeys(
    ["ctrl+z", "meta+z"],
    (event) => {
      // If a modal is open, we want the default browser undo behavior, so prevent
      // the Builder's undo from firing.
      // This can not be combined with the "beforeinput" event listener, because that
      // event is not fired when the user presses ctrl+z in fields that use monaco editors,
      // since monaco editors are not native inputs.
      if (isAnyModalOpen()) {
        return;
      }
      event.preventDefault();
      undo();
    },
    { enableOnFormTags: ["input", "textarea", "select"] }
  );

  useHotkeys(
    ["ctrl+shift+z", "meta+shift+z"],
    (event) => {
      // If a modal is open, we want the default browser redo behavior, so prevent
      // the Builder's redo from firing.
      // This can not be combined with the "beforeinput" event listener, because that
      // event is not fired when the user presses ctrl+shift+z in fields that use monaco
      // editors, since monaco editors are not native inputs.
      if (isAnyModalOpen()) {
        return;
      }
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
