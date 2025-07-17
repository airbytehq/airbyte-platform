import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem, WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";

interface SourceConnectionTableProps {
  connections: WebBackendConnectionListItem[];
  hasNextPage: boolean;
  fetchNextPage: () => void;
  setSortKey: (key: WebBackendConnectionListSortKey) => void;
  sortKey: WebBackendConnectionListSortKey;
}

const SourceConnectionTable: React.FC<SourceConnectionTableProps> = ({
  connections,
  hasNextPage,
  fetchNextPage,
  setSortKey,
  sortKey,
}) => {
  const data = getConnectionTableData(connections, "source");

  return (
    <ConnectionTable
      data={data}
      entity="source"
      hasNextPage={hasNextPage}
      fetchNextPage={fetchNextPage}
      setSortKey={setSortKey}
      sortKey={sortKey}
    />
  );
};

export default SourceConnectionTable;
