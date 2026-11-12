import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./RowError.module.scss";
import { SecretStorageRow } from "./types";

interface RowErrorProps {
  error: SecretStorageRow["error"];
}

export const RowError: React.FC<RowErrorProps> = ({ error }) => {
  if (!error) {
    return null;
  }

  return (
    <Text size="sm" className={styles.error}>
      <FormattedMessage
        id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.error"
        values={{ code: error.code, message: error.message }}
      />
    </Text>
  );
};
