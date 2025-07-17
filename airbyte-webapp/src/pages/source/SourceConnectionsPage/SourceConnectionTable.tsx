import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";
import { ScrollParent } from "components/ui/ScrollParent";

import { WebBackendConnectionListItem, WebBackendConnectionListSortKey } from "core/api/types/AirbyteClient";

import styles from "./SourceConnectionTable.module.scss";

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
    <ScrollParent props={{ className: styles.container }}>
      <ConnectionTable
        data={data}
        entity="source"
        hasNextPage={hasNextPage}
        fetchNextPage={fetchNextPage}
        setSortKey={setSortKey}
        sortKey={sortKey}
      />
    </ScrollParent>
  );
};

export default SourceConnectionTable;
