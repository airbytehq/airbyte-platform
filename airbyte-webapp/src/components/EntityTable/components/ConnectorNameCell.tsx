import React from "react";

import { ConnectorIcon } from "components/common/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./ConnectorNameCell.module.scss";
import { EntityNameCell } from "./EntityNameCell";

interface ConnectorNameCellProps {
  enabled: boolean;
  /**
   * connector name defined by user
   * @example: "My PokeAPI", "Postgres - Source", "Shopify_123"
   */
  value: string;
  /**
   * the actual name of connector
   * @example: "PokeAPI", "Postgres", "Shopify"
   */
  actualName?: string;
  icon: string | undefined;
  hideIcon?: boolean;
}

export const ConnectorNameCell: React.FC<ConnectorNameCellProps> = ({ value, actualName, enabled, icon, hideIcon }) => (
  <FlexContainer alignItems="center">
    {actualName ? (
      <Tooltip control={<ConnectorIcon icon={icon} />}>{actualName}</Tooltip>
    ) : (
      !hideIcon && <ConnectorIcon icon={icon} />
    )}
    <EntityNameCell className={styles.text} value={value} enabled={enabled} />
  </FlexContainer>
);
