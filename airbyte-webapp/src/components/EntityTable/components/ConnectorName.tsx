import React from "react";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./ConnectorName.module.scss";
import { EntityNameCell } from "./EntityNameCell";

interface ConnectorNameProps {
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

export const ConnectorName: React.FC<ConnectorNameProps> = ({ value, actualName, enabled, icon, hideIcon }) => (
  <FlexContainer alignItems="center">
    {!hideIcon && (
      <>
        {actualName ? (
          <Tooltip control={<ConnectorIcon icon={icon} />}>{actualName}</Tooltip>
        ) : (
          <ConnectorIcon icon={icon} />
        )}
      </>
    )}
    <EntityNameCell className={styles.text} value={value} enabled={enabled} />
  </FlexContainer>
);
