import React from "react";
import { useNavigate } from "react-router-dom";

import { ConnectionTable } from "components/EntityTable";
import { ConnectionTableDataItem } from "components/EntityTable/types";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

interface ConnectionsTableProps {
  connections: WebBackendConnectionListItem[];
  variant?: React.ComponentProps<typeof ConnectionTable>["variant"];
}

const ConnectionsTable: React.FC<ConnectionsTableProps> = ({ connections, variant }) => {
  const navigate = useNavigate();

  const data = getConnectionTableData(connections, "connection");

  const clickRow = (source: ConnectionTableDataItem) => navigate(`${source.connectionId}`);

  return <ConnectionTable data={data} onClickRow={clickRow} entity="connection" variant={variant} />;
};

export default ConnectionsTable;
