import React from "react";
import { useEffectOnce } from "react-use";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { trackTiming } from "core/utils/datadog";

interface ConnectionsTableProps {
  connections: WebBackendConnectionListItem[];
  variant?: React.ComponentProps<typeof ConnectionTable>["variant"];
}

const ConnectionsTable: React.FC<ConnectionsTableProps> = React.memo(({ connections, variant }) => {
  useEffectOnce(() => {
    trackTiming("ConnectionsTable");
  });

  const data = getConnectionTableData(connections, "connection");

  return <ConnectionTable data={data} entity="connection" variant={variant} />;
});
ConnectionsTable.displayName = "ConnectionsTable";
export default ConnectionsTable;
