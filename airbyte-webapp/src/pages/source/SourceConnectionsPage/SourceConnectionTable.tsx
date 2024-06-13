import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./SourceConnectionTable.module.scss";

interface IProps {
  connections: WebBackendConnectionListItem[];
}

const SourceConnectionTable: React.FC<IProps> = ({ connections }) => {
  const data = getConnectionTableData(connections, "source");

  return (
    <div className={styles.content}>
      <ConnectionTable data={data} entity="source" />
    </div>
  );
};

export default SourceConnectionTable;
