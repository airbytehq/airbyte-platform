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
  selectedFields: number;
  totalFields: number;
  variant: FieldSelectionStatusVariant;
}

export const FieldSelectionStatus: React.FC<FieldSelectionStatusProps> = ({
  selectedFields,
  totalFields,
  variant = "grey",
}) => {
  return (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      className={classnames(styles.container, STYLES_BY_VARIANT[variant])}
    >
      <FlexContainer alignItems="baseline" justifyContent="center" gap="none">
        <Text size="xs" className={styles.selected}>
          {selectedFields === totalFields ? <FormattedMessage id="form.all" /> : selectedFields}
        </Text>
        {selectedFields !== totalFields && <Text className={styles.total}>\{totalFields}</Text>}
      </FlexContainer>
    </FlexContainer>
  );
};
