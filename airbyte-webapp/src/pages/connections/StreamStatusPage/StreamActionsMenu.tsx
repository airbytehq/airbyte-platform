import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { AirbyteStream } from "core/request/AirbyteClient";
import { ConnectionRoutePaths } from "pages/routePaths";

interface StreamActionsMenuProps {
  stream?: AirbyteStream;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ stream }) => {
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
        state: { namespace: stream?.namespace, streamName: stream?.name, action: value },
      });
    }

    if (value === "resetThisStream" && stream) {
      await resetStreams([{ streamNamespace: stream?.namespace ?? "", streamName: stream?.name }]);
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
    </DropdownMenu>
  );
};
