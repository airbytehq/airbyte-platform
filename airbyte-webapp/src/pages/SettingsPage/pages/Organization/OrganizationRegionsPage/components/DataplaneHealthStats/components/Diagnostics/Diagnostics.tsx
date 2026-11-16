import React, { useId, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import { SECRET_STORAGE_CHECK_INTERVAL_MINUTES, SECRET_STORAGE_KEY, WORKLOAD_SCHEDULING_KEY } from "./constants";
import { DiagnosticKey } from "./types";
import {
  getSecretStorageCardData,
  getSecretStorageRows,
  getWorkloadSchedulingCardData,
  getWorkloadSchedulingDetails,
} from "./utils";
import { AgeMessage } from "../../types";
import { Age } from "../Age";
import { SecretStorageDetails } from "../SecretStorageDetails";
import { StatusCard, StatusCardGrid } from "../StatusCard";
import { WorkloadSchedulingDetails } from "../WorkloadSchedulingDetails";

// The caption of a check that has been observed. A card without a check to date has none.
const observedCaption = (lastChecked?: AgeMessage) =>
  lastChecked && (
    <FormattedMessage
      id="settings.organization.regions.dataplane.diagnostics.observed"
      values={{ age: <Age age={lastChecked} /> }}
    />
  );

interface DiagnosticsProps {
  dataplane: DataplaneHealthRead;
}

export const Diagnostics: React.FC<DiagnosticsProps> = ({ dataplane }) => {
  const now = useCurrentTime();
  // Only one set of details is open at a time.
  const [expandedKey, setExpandedKey] = useState<DiagnosticKey>();
  const secretStoragePanelId = useId();
  const workloadSchedulingPanelId = useId();
  const secretStorage = getSecretStorageCardData(dataplane, now);
  const secretStorageRows = getSecretStorageRows(dataplane, now);
  const hasSecretStorageDetails = secretStorageRows.length > 0;
  const workloadScheduling = getWorkloadSchedulingCardData(dataplane, now);
  const workloadSchedulingDetails = getWorkloadSchedulingDetails(dataplane, now);

  if (!secretStorage && !workloadScheduling) {
    return null;
  }

  const toggle = (key: DiagnosticKey) => () => setExpandedKey((current) => (current === key ? undefined : key));

  const toCardDetails = (key: DiagnosticKey, panelId: string, hasDetails: boolean) => {
    if (!hasDetails) {
      return undefined;
    }

    return { isExpanded: expandedKey === key, onToggle: toggle(key), panelId };
  };

  const secretStorageCardDetails = toCardDetails(SECRET_STORAGE_KEY, secretStoragePanelId, hasSecretStorageDetails);
  const workloadSchedulingCardDetails = toCardDetails(
    WORKLOAD_SCHEDULING_KEY,
    workloadSchedulingPanelId,
    workloadSchedulingDetails !== undefined
  );

  return (
    <>
      <Text size="sm" bold>
        <FormattedMessage id="settings.organization.regions.dataplane.diagnostics" />
      </Text>

      <StatusCardGrid>
        {secretStorage && (
          <StatusCard
            label={<FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.label" />}
            level={secretStorage.level}
            value={secretStorage.value}
            caption={observedCaption(secretStorage.lastChecked)}
            details={secretStorageCardDetails}
          />
        )}

        {workloadScheduling && (
          <StatusCard
            label={
              <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.label" />
            }
            level={workloadScheduling.level}
            value={workloadScheduling.value}
            caption={observedCaption(workloadScheduling.lastChecked)}
            details={workloadSchedulingCardDetails}
          />
        )}
      </StatusCardGrid>

      {hasSecretStorageDetails && expandedKey === SECRET_STORAGE_KEY && (
        <div id={secretStoragePanelId}>
          <SecretStorageDetails rows={secretStorageRows} checkIntervalMinutes={SECRET_STORAGE_CHECK_INTERVAL_MINUTES} />
        </div>
      )}

      {workloadSchedulingDetails && expandedKey === WORKLOAD_SCHEDULING_KEY && (
        <div id={workloadSchedulingPanelId}>
          <WorkloadSchedulingDetails data={workloadSchedulingDetails} />
        </div>
      )}
    </>
  );
};
