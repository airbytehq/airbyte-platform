import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";

import { SvgIcon } from "area/connector/utils";
import { SupportLevel } from "core/api/types/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";

import styles from "./ConnectorCell.module.scss";
import { ConnectorsViewProps } from "./ConnectorsView";
import { DestinationUpdateIndicator } from "./DestinationUpdateIndicator";
import { SourceUpdateIndicator } from "./SourceUpdateIndicator";

export interface ConnectorCellProps {
  connectorName: string;
  img?: string;
  supportLevel?: SupportLevel;
  custom?: boolean;
  currentVersion: string;
  type: ConnectorsViewProps["type"];
  id: string;
}

export const ConnectorCell: React.FC<ConnectorCellProps> = React.memo(
  ({ connectorName, img, supportLevel, custom = false, type, id, currentVersion }) => {
    const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors);

    return (
      <FlexContainer alignItems="center" gap="lg">
        {allowUpdateConnectors && type === "sources" && (
          <SourceUpdateIndicator id={id} currentVersion={currentVersion} custom={custom} />
        )}
        {allowUpdateConnectors && type === "destinations" && (
          <DestinationUpdateIndicator id={id} currentVersion={currentVersion} />
        )}
        <div className={styles.iconContainer}>
          <SvgIcon svg={img} />
        </div>
        <div>{connectorName}</div>
        <SupportLevelBadge tooltip={false} supportLevel={supportLevel} custom={custom} />
      </FlexContainer>
    );
  }
);
ConnectorCell.displayName = "ConnectorCell";
