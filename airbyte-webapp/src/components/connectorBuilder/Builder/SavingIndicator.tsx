import React, { useEffect, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { SavingState, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SavingIndicator.module.scss";
import { VersionModal } from "../VersionModal";

function getMessage(savingState: SavingState, displayedVersion: number | undefined, triggerUpdate: () => void) {
  if (savingState === "invalid") {
    return (
      <Tooltip
        placement="bottom-start"
        control={
          <span className={styles.invalid}>
            <FormattedMessage id="connectorBuilder.loadingState.invalid" />
          </span>
        }
      >
        <FormattedMessage id="connectorBuilder.loadingState.invalid.tooltip" />
      </Tooltip>
    );
  }
  if (savingState === "readonly") {
    return (
      <Tooltip
        placement="bottom-start"
        control={
          <span className={styles.invalid}>
            <FormattedMessage id="connectorBuilder.loadingState.readonly" />
          </span>
        }
      >
        <FormattedMessage id="connectorBuilder.loadingState.readonly.tooltip" />
      </Tooltip>
    );
  }
  if (savingState === "loading") {
    return (
      <FlexContainer gap="sm" alignItems="center">
        <Spinner size="xs" />
        <FormattedMessage id="connectorBuilder.loadingState.loading" />
      </FlexContainer>
    );
  }
  if (savingState === "error") {
    return (
      <button type="button" className={styles.retryButton} onClick={triggerUpdate}>
        <FormattedMessage id="connectorBuilder.loadingState.error" />
      </button>
    );
  }
  return (
    <FlexContainer gap="sm" alignItems="center">
      <Icon type="check" />
      <FlexItem>
        {displayedVersion ? <>v{displayedVersion}</> : <FormattedMessage id="connectorBuilder.loadingState.saved" />}
      </FlexItem>
    </FlexContainer>
  );
}

export const SavingIndicator: React.FC = () => {
  const { savingState, triggerUpdate, currentProject, displayedVersion } = useConnectorBuilderFormState();
  const [pendingUpdate, setPendingUpdate] = useState(false);
  const [changeInProgress, setChangeInProgress] = useState(false);
  const timeoutRef = useRef<number>();

  useEffect(
    () => () => {
      if (timeoutRef.current) {
        window.clearTimeout(timeoutRef.current);
      }
    },
    []
  );

  const isPublished = Boolean(currentProject.sourceDefinitionId);

  const message = (
    <Text size="sm" color="grey" as="div" className={styles.text}>
      {getMessage(pendingUpdate ? "loading" : savingState, displayedVersion, () => {
        setPendingUpdate(true);
        if (timeoutRef.current) {
          window.clearTimeout(timeoutRef.current);
        }
        // wait for 200ms before actually triggering the update so the user can see their action
        // took effect even if the update fails very quickly.
        timeoutRef.current = window.setTimeout(() => {
          setPendingUpdate(false);
          triggerUpdate();
        }, 200);
      })}
    </Text>
  );

  if (!isPublished || savingState === "error") {
    return message;
  }

  return (
    <>
      <Button
        type="button"
        variant="clear"
        onClick={() => {
          setChangeInProgress(true);
        }}
        icon={<Icon type="chevronDown" />}
        iconPosition="right"
      >
        {message}
      </Button>
      {changeInProgress && <VersionModal onClose={() => setChangeInProgress(false)} project={currentProject} />}
    </>
  );
};
