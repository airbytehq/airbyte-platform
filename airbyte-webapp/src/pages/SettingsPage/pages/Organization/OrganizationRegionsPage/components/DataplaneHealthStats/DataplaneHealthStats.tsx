import React from "react";
import { FormattedMessage } from "react-intl";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import { Diagnostics } from "./components/Diagnostics";
import { StatusCard, StatusCardGrid } from "./components/StatusCard";
import styles from "./DataplaneHealthStats.module.scss";
import { getCompatibilityCardData, getLastCheckInCardData, getVersionCardData } from "./utils";
import { healthStatusBadgeVariant, healthStatusMessageId } from "../../healthStatus";
import { HealthHistoryBar } from "../HealthHistoryBar";

interface DataplaneHealthStatsProps {
  dataplane: DataplaneHealthRead;
}

const LastCheckInCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => {
  const now = useCurrentTime();

  return (
    <StatusCard
      label={<FormattedMessage id="settings.organization.regions.dataplane.lastCheckIn" />}
      {...getLastCheckInCardData(dataplane, now)}
    />
  );
};

const AirbyteVersionCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => (
  <StatusCard
    label={<FormattedMessage id="settings.organization.regions.dataplane.version" />}
    {...getVersionCardData(dataplane)}
  />
);

const CompatibilityCard: React.FC<DataplaneHealthStatsProps> = ({ dataplane }) => {
  const cardData = getCompatibilityCardData(dataplane);

  if (!cardData) {
    return null;
  }

  return (
    <StatusCard label={<FormattedMessage id="settings.organization.regions.dataplane.compatibility" />} {...cardData} />
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

      <StatusCardGrid>
        <LastCheckInCard dataplane={dataplane} />
        <AirbyteVersionCard dataplane={dataplane} />
        <CompatibilityCard dataplane={dataplane} />
      </StatusCardGrid>

      <Diagnostics dataplane={dataplane} />

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
