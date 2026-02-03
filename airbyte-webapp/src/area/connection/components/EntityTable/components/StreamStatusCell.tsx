import React from "react";
import { FormattedMessage } from "react-intl";

import { Tooltip } from "components/ui/Tooltip";

import { ConnectionStatusIndicator } from "area/connection/components/ConnectionStatusIndicator";
import { useGetCachedConnectionStatusesById } from "core/api";

export const StreamsStatusCell: React.FC<{ connectionId: string }> = ({ connectionId }) => {
  const status = useGetCachedConnectionStatusesById([connectionId])[connectionId]?.connectionSyncStatus;

  return (
    <Tooltip control={<ConnectionStatusIndicator status={status} size="sm" />}>
      <FormattedMessage id={`connection.status.${status}`} />
    </Tooltip>
  );
};
