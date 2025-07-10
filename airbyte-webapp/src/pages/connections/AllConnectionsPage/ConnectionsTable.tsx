import React from "react";
import { useEffectOnce } from "react-use";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem, WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";
import { trackTiming } from "core/utils/datadog";

interface ConnectionsTableProps {
  connections: WebBackendConnectionListItem[];
  variant?: React.ComponentProps<typeof ConnectionTable>["variant"];
  sortKey?: WebBackendConnectionListSortKey;
  setSortKey?: (sortState: WebBackendConnectionListSortKey) => void;
  hasNextPage: boolean;
  fetchNextPage: () => void;
}

const ConnectionsTable: React.FC<ConnectionsTableProps> = React.memo(
  ({ connections, variant, sortKey, setSortKey, hasNextPage, fetchNextPage }) => {
    useEffectOnce(() => {
      trackTiming("ConnectionsTable");
    });

    const data = getConnectionTableData(connections, "connection");

    return (
      <ConnectionTable
        data={data}
        entity="connection"
        variant={variant}
        sortKey={sortKey}
        setSortKey={setSortKey}
        hasNextPage={hasNextPage}
        fetchNextPage={fetchNextPage}
      />
    );
  }
);
ConnectionsTable.displayName = "ConnectionsTable";
export default ConnectionsTable;
