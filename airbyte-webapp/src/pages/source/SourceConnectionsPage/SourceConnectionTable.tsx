import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";
import { ScrollParent } from "components/ui/ScrollParent";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./SourceConnectionTable.module.scss";

interface SourceConnectionTableProps {
  connections: WebBackendConnectionListItem[];
}

const SourceConnectionTable: React.FC<SourceConnectionTableProps> = ({ connections }) => {
  const data = getConnectionTableData(connections, "source");

  return (
    <ScrollParent props={{ className: styles.container }}>
      <ConnectionTable data={data} entity="source" />
    </ScrollParent>
  );
};

export default SourceConnectionTable;
