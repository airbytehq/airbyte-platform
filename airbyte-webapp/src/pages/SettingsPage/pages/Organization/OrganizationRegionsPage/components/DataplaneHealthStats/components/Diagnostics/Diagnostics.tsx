import React, { useId, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";
import { useCurrentTime } from "core/utils/time";

import { getSecretStorageCardData, getSecretStorageRows } from "./utils";
import { SecretStorageDetails } from "../SecretStorageDetails";
import { StatusCard, StatusCardGrid } from "../StatusCard";

const SECRET_STORAGE_KEY = "secretStorage";

// The launcher probes each secret store at this rate (workload-launcher.secret-storage-probe.rate: PT30M).
const SECRET_STORAGE_CHECK_INTERVAL_MINUTES = 30;

type DiagnosticKey = typeof SECRET_STORAGE_KEY;

interface DiagnosticsProps {
  dataplane: DataplaneHealthRead;
}

export const Diagnostics: React.FC<DiagnosticsProps> = ({ dataplane }) => {
  const now = useCurrentTime();
  // Only one set of details is open at a time.
  const [expanded, setExpanded] = useState<DiagnosticKey>();
  const secretStoragePanelId = useId();
  const secretStorage = getSecretStorageCardData(dataplane, now);
  const secretStorageRows = getSecretStorageRows(dataplane, now);
  const hasSecretStorageDetails = secretStorageRows.length > 0;

  if (!secretStorage) {
    return null;
  }

  const toggle = (key: DiagnosticKey) => () => setExpanded((current) => (current === key ? undefined : key));

  const secretStorageCaption = secretStorage.lastChecked && (
    <FormattedMessage
      id="settings.organization.regions.dataplane.diagnostics.observed"
      values={{
        age: <FormattedMessage id={secretStorage.lastChecked.messageId} values={secretStorage.lastChecked.values} />,
      }}
    />
  );

  return (
    <>
      <Text size="sm" bold>
        <FormattedMessage id="settings.organization.regions.dataplane.diagnostics" />
      </Text>

      <StatusCardGrid>
        <StatusCard
          label={<FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.label" />}
          level={secretStorage.level}
          value={secretStorage.value}
          caption={secretStorageCaption}
          details={
            hasSecretStorageDetails
              ? {
                  isExpanded: expanded === SECRET_STORAGE_KEY,
                  onToggle: toggle(SECRET_STORAGE_KEY),
                  panelId: secretStoragePanelId,
                }
              : undefined
          }
        />
      </StatusCardGrid>

      {hasSecretStorageDetails && expanded === SECRET_STORAGE_KEY && (
        <div id={secretStoragePanelId}>
          <SecretStorageDetails rows={secretStorageRows} checkIntervalMinutes={SECRET_STORAGE_CHECK_INTERVAL_MINUTES} />
        </div>
      )}
    </>
  );
};
