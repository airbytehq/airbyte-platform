import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem, WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";

interface DestinationConnectionTableProps {
  connections: WebBackendConnectionListItem[];
  hasNextPage: boolean;
  fetchNextPage: () => void;
  setSortKey: (key: WebBackendConnectionListSortKey) => void;
  sortKey: WebBackendConnectionListSortKey;
}

export const DestinationConnectionTable: React.FC<DestinationConnectionTableProps> = ({
  connections,
  hasNextPage,
  fetchNextPage,
  setSortKey,
  sortKey,
}) => {
  const data = getConnectionTableData(connections, "destination");

  return (
    <ConnectionTable
      data={data}
      entity="destination"
      hasNextPage={hasNextPage}
      fetchNextPage={fetchNextPage}
      setSortKey={setSortKey}
      sortKey={sortKey}
    />
  );
};
