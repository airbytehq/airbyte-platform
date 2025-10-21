import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { SwitchWithLock } from "components/ui/Switch/SwitchWithLock";
import { Tooltip } from "components/ui/Tooltip";

import { useUpdateConnection } from "core/api";
import { ConnectionStatus, SchemaChange } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useAnalyticsTrackFunctions } from "hooks/services/ConnectionEdit/useAnalyticsTrackFunctions";

import { ConnectionTableDataItem } from "../types";

export const StateSwitchCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, boolean>> = (props) => {
  const connectionId = props.row.original.connectionId;
  const enabled = props.cell.getValue();
  const schemaChange = props.row.original.schemaChange;
  const connectionStatus = props.row.original.status;
  const { trackConnectionStatusUpdate } = useAnalyticsTrackFunctions();
  const canEditConnection = useGeneratedIntent(Intent.CreateOrEditConnection);
  const { mutateAsync: updateConnection, isLoading } = useUpdateConnection();

  const onChange = async ({ target: { checked } }: React.ChangeEvent<HTMLInputElement>) => {
    const updatedConnection = await updateConnection({
      connectionId,
      status: checked ? ConnectionStatus.active : ConnectionStatus.inactive,
      skipReset: true,
    });
    trackConnectionStatusUpdate(updatedConnection);
  };

  const isLocked = connectionStatus === ConnectionStatus.locked;
  const isDisabled = isLocked || schemaChange === SchemaChange.breaking || !canEditConnection || isLoading;

  const switchComponent = (
    <SwitchWithLock
      size="sm"
      checked={enabled}
      onChange={onChange}
      disabled={isDisabled}
      loading={isLoading}
      showLock={isLocked}
      data-testid={`connection-state-switch-${connectionId}`}
    />
  );

  return (
    <FlexContainer justifyContent="center">
      {isLocked ? (
        <Tooltip control={switchComponent}>
          <FormattedMessage id="connection.lockedTooltip" />
        </Tooltip>
      ) : (
        switchComponent
      )}
    </FlexContainer>
  );
};
