import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";

import { useUpdateConnection } from "core/api";
import { ConnectionStatus, SchemaChange } from "core/api/types/AirbyteClient";
import { Intent, useGeneratedIntent } from "core/utils/rbac";
import { useAnalyticsTrackFunctions } from "hooks/services/ConnectionEdit/useAnalyticsTrackFunctions";

import { ConnectionTableDataItem } from "../types";

export const StateSwitchCell: ColumnDefTemplate<CellContext<ConnectionTableDataItem, boolean>> = (props) => {
  const connectionId = props.row.original.connectionId;
  const enabled = props.cell.getValue();
  const schemaChange = props.row.original.schemaChange;
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

  const isDisabled = schemaChange === SchemaChange.breaking || !canEditConnection || isLoading;

  return (
    <FlexContainer justifyContent="center">
      <Switch
        size="sm"
        checked={enabled}
        onChange={onChange}
        disabled={isDisabled}
        loading={isLoading}
        data-testid={`connection-state-switch-${connectionId}`}
      />
    </FlexContainer>
  );
};
