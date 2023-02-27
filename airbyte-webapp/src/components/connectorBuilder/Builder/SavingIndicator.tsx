import { faCheck } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Spinner } from "components/ui/Spinner";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SavingIndicator.module.scss";

function getMessage(savingState: "invalid" | "loading" | "saved") {
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
  const { savingState } = useConnectorBuilderFormState();
  return (
    <Text size="sm" color="grey" as="div">
      {getMessage(savingState)}
    </Text>
  );
};
