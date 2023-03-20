import classnames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./FieldSelectionStatus.module.scss";

export type FieldSelectionStatusVariant = "grey" | "blue" | "green" | "red";

const STYLES_BY_VARIANT: Readonly<Record<FieldSelectionStatusVariant, string>> = {
  grey: styles.grey,
  blue: styles.blue,
  green: styles.green,
  red: styles.red,
};

interface FieldSelectionStatusProps {
  selectedFieldCount: number;
  totalFieldCount: number;
  variant: FieldSelectionStatusVariant;
}

export const FieldSelectionStatus: React.FC<FieldSelectionStatusProps> = ({
  selectedFieldCount,
  totalFieldCount,
  variant = "grey",
}) => {
  return (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      className={classnames(styles.container, STYLES_BY_VARIANT[variant])}
      data-testid="field-selection-status"
    >
      <FlexContainer alignItems="baseline" justifyContent="center" gap="none">
        <Text size="xs" className={styles.selected} data-testid="field-selection-status-selected-count">
          {selectedFieldCount === totalFieldCount ? <FormattedMessage id="form.all" /> : selectedFieldCount}
        </Text>
        {selectedFieldCount !== totalFieldCount && (
          <Text className={styles.total} data-testid="field-selection-status-total-count">
            /{totalFieldCount}
          </Text>
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
