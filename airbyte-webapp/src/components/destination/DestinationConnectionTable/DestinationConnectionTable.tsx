import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./DestinationConnectionTable.module.scss";

interface IProps {
  connections: WebBackendConnectionListItem[];
}

export const DestinationConnectionTable: React.FC<IProps> = ({ connections }) => {
  const data = getConnectionTableData(connections, "destination");

  return (
    <div className={styles.content}>
      <ConnectionTable data={data} entity="destination" />
    </div>
  );
};
