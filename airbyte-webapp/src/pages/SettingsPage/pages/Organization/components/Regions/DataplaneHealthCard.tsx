import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import styles from "./DataplaneHealthCard.module.scss";
import { HealthHistoryBar } from "./HealthHistoryBar";
import { healthStatusBadgeVariant, healthStatusIcon, HealthStatusIcon, healthStatusMessageId } from "./healthStatus";

interface DataplaneHealthCardProps {
  dataplane: DataplaneHealthRead;
}

const UNKNOWN_VERSION = "unknown";

export const formatCheckInAge = (ageMs: number): { id: string; values: Record<string, number> } => {
  const totalSeconds = Math.max(0, Math.floor(ageMs / 1000));
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (days > 0) {
    return { id: "settings.organization.regions.dataplane.checkIn.days", values: { days, hours } };
  }
  if (hours > 0) {
    return { id: "settings.organization.regions.dataplane.checkIn.hours", values: { hours, minutes, seconds } };
  }
  if (minutes > 0) {
    return { id: "settings.organization.regions.dataplane.checkIn.minutes", values: { minutes, seconds } };
  }
  return { id: "settings.organization.regions.dataplane.checkIn.seconds", values: { seconds } };
};

const TileValue: React.FC<React.PropsWithChildren<{ icon: HealthStatusIcon }>> = ({ icon, children }) => (
  <FlexContainer alignItems="center" gap="xs">
    <Icon type={icon.type} color={icon.color} size="sm" />
    <Text size="sm" bold>
      {children}
    </Text>
  </FlexContainer>
);

// A data plane without a check-in always has the UNKNOWN status.
const NeverConnected: React.FC = () => (
  <TileValue icon={healthStatusIcon.UNKNOWN}>
    <FormattedMessage id="settings.organization.regions.neverConnected" />
  </TileValue>
);

const LastCheckIn: React.FC<{ dataplane: DataplaneHealthRead }> = ({ dataplane }) => {
  const now = useCurrentTime();

  if (dataplane.last_heartbeat_timestamp === undefined) {
    return <NeverConnected />;
  }

  const age = formatCheckInAge(now - dataplane.last_heartbeat_timestamp);
  return (
    <TileValue icon={healthStatusIcon[dataplane.status]}>
      <FormattedMessage id={age.id} values={age.values} />
    </TileValue>
  );
};

// The server stores "unknown" when a check-in carries no version, so a connected data plane can lack one.
const AirbyteVersion: React.FC<{ dataplane: DataplaneHealthRead }> = ({ dataplane }) => {
  if (dataplane.last_heartbeat_timestamp === undefined) {
    return <NeverConnected />;
  }

  const version = dataplane.dataplane_version;
  if (!version || version === UNKNOWN_VERSION) {
    return (
      <Text size="sm" bold color="grey">
        <FormattedMessage id="settings.organization.regions.dataplane.version.unavailable" />
      </Text>
    );
  }
  return (
    <Text size="sm" bold>
      {version}
    </Text>
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

export const DataplaneHealthCard: React.FC<DataplaneHealthCardProps> = ({ dataplane }) => {
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

      <FlexContainer gap="md">
        <FlexItem grow className={styles.tile}>
          <Text size="sm" color="grey400">
            <FormattedMessage id="settings.organization.regions.dataplane.lastCheckIn" />
          </Text>
          <LastCheckIn dataplane={dataplane} />
        </FlexItem>
        <FlexItem grow className={styles.tile}>
          <Text size="sm" color="grey400">
            <FormattedMessage id="settings.organization.regions.dataplane.version" />
          </Text>
          <AirbyteVersion dataplane={dataplane} />
        </FlexItem>
      </FlexContainer>

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
