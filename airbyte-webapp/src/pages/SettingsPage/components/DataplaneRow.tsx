import { useEffect, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { DataplanePulseGraph } from "./DataplanePulseGraph";
import styles from "./DataplaneRow.module.scss";

interface DataplaneRowProps {
  dataplaneName: string;
  status: string;
  lastHeartbeatTimestamp?: number;
  recentHeartbeats?: Array<{ timestamp: number }>;
  dataplaneVersion?: string;
}

export const DataplaneRow: React.FC<DataplaneRowProps> = ({
  dataplaneName,
  status,
  lastHeartbeatTimestamp,
  recentHeartbeats = [],
  dataplaneVersion,
}) => {
  const { formatMessage } = useIntl();
  const [now, setNow] = useState(Date.now());

  useEffect(() => {
    const interval = setInterval(() => {
      setNow(Date.now());
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const isHealthy = status === "HEALTHY";

  const getStatusDisplay = () => {
    const statusText = isHealthy
      ? formatMessage({ id: "dataplaneHealth.status.healthy" })
      : formatMessage({ id: "dataplaneHealth.status.unhealthy" });
    const variant = isHealthy ? "green" : "red";

    return (
      <Badge variant={variant} uppercase={false}>
        {statusText}
      </Badge>
    );
  };

  const formatLastHeartbeat = () => {
    if (!lastHeartbeatTimestamp) {
      return formatMessage({ id: "dataplaneHealth.lastHeartbeat.never" });
    }
    const date = new Date(lastHeartbeatTimestamp);
    const diffMs = now - date.getTime();
    const diffSeconds = Math.floor(diffMs / 1000);
    const diffMinutes = Math.floor(diffMs / 60000);

    if (diffSeconds < 60) {
      return formatMessage({ id: "dataplaneHealth.lastHeartbeat.secondsAgo" }, { seconds: diffSeconds });
    } else if (diffMinutes < 60) {
      return formatMessage({ id: "dataplaneHealth.lastHeartbeat.minutesAgo" }, { minutes: diffMinutes });
    }
    return date.toLocaleString();
  };

  return (
    <FlexContainer className={styles.dataplaneRow} alignItems="center" justifyContent="space-between">
      <FlexContainer alignItems="center" gap="md" className={styles.leftSection}>
        <Text size="sm" className={styles.dataplaneName}>
          {dataplaneName}
        </Text>
        <div className={styles.statusContainer}>{getStatusDisplay()}</div>
        {dataplaneVersion && dataplaneVersion !== "unknown" && (
          <Text size="sm" color="grey">
            v{dataplaneVersion}
          </Text>
        )}
      </FlexContainer>
      <FlexContainer alignItems="center" gap="lg">
        {isHealthy ? (
          <>
            <DataplanePulseGraph heartbeats={recentHeartbeats} />
            <Text size="sm" color="grey" className={styles.lastHeartbeat}>
              {formatLastHeartbeat()}
            </Text>
          </>
        ) : (
          <Text size="sm" color="grey">
            <FormattedMessage id="dataplaneHealth.noData" />
          </Text>
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
