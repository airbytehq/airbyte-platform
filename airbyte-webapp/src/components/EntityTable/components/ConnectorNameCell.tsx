import React from "react";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";

import styles from "./ConnectorNameCell.module.scss";
import { EntityNameCell } from "./EntityNameCell";

interface ConnectorNameCellProps {
  enabled: boolean;
  value: string;
  icon: string | undefined;
  hideIcon?: boolean;
}

export const ConnectorNameCell: React.FC<ConnectorNameCellProps> = ({ value, enabled, icon, hideIcon }) => {
  return (
    <FlexContainer alignItems="center" title={value}>
      {!hideIcon && <ConnectorIcon icon={icon} />}
      <EntityNameCell className={styles.text} value={value} enabled={enabled} />
    </FlexContainer>
  );
};
