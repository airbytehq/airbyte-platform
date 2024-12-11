import React from "react";

import { ConnectionTable } from "components/EntityTable";
import { getConnectionTableData } from "components/EntityTable/utils";
import { ScrollParent } from "components/ui/ScrollParent";

import { WebBackendConnectionListItem } from "core/api/types/AirbyteClient";

import styles from "./DestinationConnectionTable.module.scss";

interface DestinationConnectionTableProps {
  connections: WebBackendConnectionListItem[];
}

export const DestinationConnectionTable: React.FC<DestinationConnectionTableProps> = ({ connections }) => {
  const data = getConnectionTableData(connections, "destination");

  return (
    <ScrollParent props={{ className: styles.container }}>
      <ConnectionTable data={data} entity="destination" />
    </ScrollParent>
  );
};
