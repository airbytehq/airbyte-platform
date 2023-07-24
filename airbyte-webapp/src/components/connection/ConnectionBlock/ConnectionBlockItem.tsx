import React from "react";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./ConnectionBlockItem.module.scss";

interface IProps {
  name?: string;
  icon?: string;
}

const ConnectionBlockItem: React.FC<IProps> = (props) => {
  return (
    <FlexContainer alignItems="center" className={styles.content} gap="md">
      {props.icon && <ConnectorIcon icon={props.icon} />}
      {props.name && (
        <Text size="lg" className={styles.name}>
          {props.name}
        </Text>
      )}
    </FlexContainer>
  );
};

export { ConnectionBlockItem };
