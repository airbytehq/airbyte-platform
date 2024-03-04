import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ConnectionsSummary.module.scss";

export type SummaryKey = "healthy" | "failed" | "paused" | "running";

export const connectionStatColors: Record<SummaryKey, React.ComponentPropsWithoutRef<typeof Text>["color"]> = {
  healthy: "green600",
  failed: "red",
  paused: "grey",
  running: "blue",
};

export const ConnectionsSummary: React.FC<Record<SummaryKey, number>> = (props) => {
  const keys = Object.keys(props) as SummaryKey[];
  const parts: React.ReactNode[] = [];
  const connectionsCount = keys.reduce((total, value) => total + props[value], 0);
  let consumedConnections = 0;

  for (const key of keys) {
    const value = props[key];
    if (value) {
      consumedConnections += value;
      parts.push(
        <Text key={key} as="span" size="lg" color={connectionStatColors[key]} className={styles.lowercase}>
          {value} <FormattedMessage id={`tables.connections.filters.status.${key}`} />
        </Text>,
        consumedConnections < connectionsCount && (
          <Text key={`${key}-middot`} as="span" size="lg" bold color="grey">
            &nbsp;&middot;&nbsp;
          </Text>
        )
      );
    }
  }

  return <>{parts}</>;
};
