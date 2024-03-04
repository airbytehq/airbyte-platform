import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { NumberBadge } from "components/ui/NumberBadge";
import { Text } from "components/ui/Text";

import styles from "./ConnectEntitiesCell.module.scss";

interface IProps {
  values: Array<{
    name: string;
    connector: string;
  }>;
  enabled?: boolean;
  entity: "source" | "destination";
}

const ConnectEntitiesCell: React.FC<IProps> = ({ values, enabled, entity }) => {
  return (
    <FlexContainer alignItems="center" gap="sm" className={classNames(styles.content, { [styles.disabled]: !enabled })}>
      <NumberBadge value={values.length} />
      {values.length > 0 && (
        <FlexItem className={styles.connectorsContainer}>
          {values.length === 1 ? (
            <Text size="sm">{values[0].name}</Text>
          ) : (
            <Text size="sm">
              <FormattedMessage id={`tables.${entity}ConnectWithNum`} values={{ num: values.length }} />
            </Text>
          )}
          <Text size="sm" color="grey300" className={styles.connectors}>
            {values.map((value) => value.connector).join(", ")}
          </Text>
        </FlexItem>
      )}
    </FlexContainer>
  );
};

export default ConnectEntitiesCell;
