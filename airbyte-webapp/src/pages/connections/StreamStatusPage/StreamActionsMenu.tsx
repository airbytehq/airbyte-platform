import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { StreamWithStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { ConnectionRoutePaths } from "pages/routePaths";

interface StreamActionsMenuProps {
  streamState?: StreamWithStatus | undefined;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamState }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();

  const options: DropdownMenuOptionType[] = [
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
    navigate(`../${ConnectionRoutePaths.Replication}`, {
      state: { namespace: streamState?.streamNamespace, streamName: streamState?.streamName, action: value },
    });
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
    </DropdownMenu>
  );
};
