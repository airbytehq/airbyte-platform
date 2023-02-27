import { faCheck } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { FormattedMessage } from "react-intl";

import { StatusIcon } from "components/ui/StatusIcon";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./SavingIndicator.module.scss";

export const SavingIndicator: React.FC = () => {
  const { savingState } = useConnectorBuilderFormState();
  if (savingState === "invalid") {
    return (
      <span className={styles.invalid}>
        <FormattedMessage id="connectorBuilder.loadingState.invalid" />
      </span>
    );
  }
  if (savingState === "loading") {
    return (
      <>
        <StatusIcon status="loading" />
        <FormattedMessage id="connectorBuilder.loadingState.loading" />
      </>
    );
  }
  return (
    <>
      <FontAwesomeIcon icon={faCheck} />
      <FormattedMessage id="connectorBuilder.loadingState.saved" />
    </>
  );
};
