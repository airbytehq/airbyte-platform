import { faEllipsisV } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React from "react";
import { useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { AirbyteStream } from "core/request/AirbyteClient";

import { ConnectionRoutePaths } from "../types";

interface StreamActionsMenuProps {
  stream?: AirbyteStream;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ stream }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();

  const options: DropdownMenuOptionType[] = [
    {
      as: "button",
      displayName: formatMessage({ id: "connection.stream.actions.resetThisStream" }),
    },
    {
      as: "button",
      displayName: formatMessage({ id: "connection.stream.actions.showInReplicationTable" }),
      value: "showInReplicationTable",
    },
    {
      as: "button",
      displayName: formatMessage({ id: "connection.stream.actions.openDetails" }),
    },
  ];

  const onOptionClick = (option: DropdownMenuOptionType) => {
    if (option.value === "showInReplicationTable") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: stream?.namespace, streamName: stream?.name },
      });
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon={<FontAwesomeIcon icon={faEllipsisV} />} />}
    </DropdownMenu>
  );
};
