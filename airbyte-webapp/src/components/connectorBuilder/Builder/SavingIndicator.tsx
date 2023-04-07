import { faCheck } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { useEffect, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { SavingState, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SavingIndicator.module.scss";

function getMessage(savingState: SavingState, triggerUpdate: () => void) {
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
      <FontAwesomeIcon icon={faCheck} />
      <FlexItem>
        <FormattedMessage id="connectorBuilder.loadingState.saved" />
      </FlexItem>
    </FlexContainer>
  );
}

export const SavingIndicator: React.FC = () => {
  const { savingState, triggerUpdate } = useConnectorBuilderFormState();
  const [pendingUpdate, setPendingUpdate] = useState(false);
  const timeoutRef = useRef<number>();

  useEffect(
    () => () => {
      if (timeoutRef.current) {
        window.clearTimeout(timeoutRef.current);
      }
    },
    []
  );

  return (
    <Text size="sm" color="grey" as="div" className={styles.text}>
      {getMessage(pendingUpdate ? "loading" : savingState, () => {
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
};
