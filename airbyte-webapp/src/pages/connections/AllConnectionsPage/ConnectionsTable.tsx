import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

interface ConnectionsTableProps {
  connections: WebBackendConnectionListItem[];
  variant?: React.ComponentProps<typeof ConnectionTable>["variant"];
}

const ConnectionsTable: React.FC<ConnectionsTableProps> = ({ connections, variant }) => {
  const data = getConnectionTableData(connections, "connection");

  return <ConnectionTable data={data} entity="connection" variant={variant} />;
};

export default ConnectionsTable;
