import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { AirbyteStream } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useResetConnectionStream } from "hooks/services/useConnectionHook";

import { ConnectionRoutePaths } from "../types";

interface StreamActionsMenuProps {
  stream?: AirbyteStream;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ stream }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();

  const { connection } = useConnectionEditService();
  const { syncStarting, jobSyncRunning, resetStarting, jobResetRunning } = useConnectionSyncContext();
  const { mutateAsync: resetStream } = useResetConnectionStream(connection.connectionId);

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
    },
  ];

  const onOptionClick = async (option: DropdownMenuOptionType) => {
    if (option.value === "showInReplicationTable") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: stream?.namespace, streamName: stream?.name },
      });
    }

    if (option.value === "resetThisStream" && stream) {
      await resetStream([{ streamNamespace: stream?.namespace || "", streamName: stream?.name }]);
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
    </DropdownMenu>
  );
};
