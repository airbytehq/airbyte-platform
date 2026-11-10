import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import { StatusCard, StatusLevel } from "./components/StatusCard";
import styles from "./DataplaneHealthStats.module.scss";
import { AgeMessage, CardData, formatAge, getSecretStorageCardData, NEVER_CONNECTED } from "./utils";
import { healthStatusBadgeVariant, healthStatusMessageId } from "../../healthStatus";
import { HealthHistoryBar } from "../HealthHistoryBar";

interface DataplaneHealthStatsProps {
  dataplane: DataplaneHealthRead;
}

const UNKNOWN_VERSION = "unknown";

const LEVEL_BY_HEALTH_STATUS: Record<DataplaneHealthRead["status"], StatusLevel> = {
  HEALTHY: "success",
  DEGRADED: "warning",
  UNHEALTHY: "error",
  UNKNOWN: "neutral",
};

const getLastCheckInCardData = (dataplane: DataplaneHealthRead, now: number): CardData => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  return {
    level: LEVEL_BY_HEALTH_STATUS[dataplane.status],
    value: formatAge(now - dataplane.last_heartbeat_timestamp),
  };
};

const LastCheckInCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => {
  const now = useCurrentTime();

  return (
    <StatusCard
      label={<FormattedMessage id="settings.organization.regions.dataplane.lastCheckIn" />}
      {...getLastCheckInCardData(dataplane, now)}
    />
  );
};

// The server stores "unknown" when a check-in carries no version, so a connected data plane can lack one.
const getVersionCardData = (dataplane: DataplaneHealthRead): CardData => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return NEVER_CONNECTED;
  }

  const version = dataplane.dataplane_version;
  if (!version || version === UNKNOWN_VERSION) {
    return {
      level: "neutral",
      value: { messageId: "settings.organization.regions.dataplane.version.unavailable" },
    };
  }

  return { level: "success", value: { text: version } };
};

const AirbyteVersionCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => (
  <StatusCard
    label={<FormattedMessage id="settings.organization.regions.dataplane.version" />}
    {...getVersionCardData(dataplane)}
  />
);

interface ObservedAgeProps {
  age: AgeMessage;
}

const ObservedAge: React.FC<ObservedAgeProps> = ({ age }) => (
  <FormattedMessage
    id="settings.organization.regions.dataplane.diagnostics.observed"
    values={{ age: <FormattedMessage id={age.messageId} values={age.values} /> }}
  />
);

const SecretStorageCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => {
  const now = useCurrentTime();
  const secretStorage = getSecretStorageCardData(dataplane, now);

  if (!secretStorage) {
    return null;
  }

  return (
    <>
      <Text size="sm" bold>
        <FormattedMessage id="settings.organization.regions.dataplane.diagnostics" />
      </Text>

      <div className={styles.cards}>
        <StatusCard
          label={<FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.label" />}
          level={secretStorage.level}
          value={secretStorage.value}
          caption={secretStorage.lastChecked && <ObservedAge age={secretStorage.lastChecked} />}
        />
      </div>
    </>
  );
};

const EnabledBadge: React.FC<{ enabled?: boolean }> = ({ enabled }) => {
  if (enabled === undefined) {
    return null;
  }
  if (enabled) {
    return (
      <Badge variant="green" uppercase={false}>
        <FormattedMessage id="settings.organization.regions.dataplane.enabled" />
      </Badge>
    );
  }
  return (
    <Badge variant="red" uppercase={false}>
      <FormattedMessage id="settings.organization.regions.dataplane.disabled" />
    </Badge>
  );
};

export const DataplaneHealthStats: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => {
  return (
    <FlexContainer direction="column" gap="md" className={styles.card} data-testid="dataplane-health-card">
      <FlexContainer alignItems="center" justifyContent="space-between">
        <Text size="lg" bold>
          {dataplane.dataplane_name}
        </Text>
        <FlexContainer alignItems="center" gap="sm">
          <Badge variant={healthStatusBadgeVariant[dataplane.status]} uppercase={false}>
            <FormattedMessage id={healthStatusMessageId[dataplane.status]} />
          </Badge>
          <EnabledBadge enabled={dataplane.enabled} />
        </FlexContainer>
      </FlexContainer>

      <div className={styles.cards}>
        <LastCheckInCard dataplane={dataplane} />
        <AirbyteVersionCard dataplane={dataplane} />
      </div>

      <SecretStorageCard dataplane={dataplane} />

      <FlexContainer direction="column" gap="sm">
        <Text size="sm" bold>
          <FormattedMessage id="settings.organization.regions.dataplane.history" />
        </Text>
        <div className={styles.tile}>
          <HealthHistoryBar history={dataplane.health_history} />
        </div>
      </FlexContainer>
    </FlexContainer>
  );
};
