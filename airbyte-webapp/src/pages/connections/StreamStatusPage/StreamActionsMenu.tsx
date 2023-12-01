import React from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { StreamWithStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { ConnectionRoutePaths } from "pages/routePaths";

interface StreamActionsMenuProps {
  streamState?: StreamWithStatus | undefined;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamState }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();

  const { syncStarting, jobSyncRunning, resetStarting, jobResetRunning, resetStreams } = useConnectionSyncContext();

  const options: DropdownMenuOptionType[] = [
    {
      displayName: formatMessage({ id: "connection.stream.actions.resetThisStream" }),
      value: "resetThisStream",
      disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning,
    },
    {
      displayName: formatMessage({ id: "connection.stream.actions.showInReplicationTable" }),
      value: "showInReplicationTable",
    },
    {
      displayName: formatMessage({ id: "connection.stream.actions.openDetails" }),
      value: "openDetails",
    },
  ];

  const onOptionClick = async ({ value }: DropdownMenuOptionType) => {
    if (value === "showInReplicationTable" || value === "openDetails") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: streamState?.streamNamespace, streamName: streamState?.streamName, action: value },
      });
    }

    if (value === "resetThisStream" && streamState) {
      await resetStreams([{ streamNamespace: streamState.streamNamespace, streamName: streamState.streamName }]);
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon={<Icon type="options" />} />}
    </DropdownMenu>
  );
};
