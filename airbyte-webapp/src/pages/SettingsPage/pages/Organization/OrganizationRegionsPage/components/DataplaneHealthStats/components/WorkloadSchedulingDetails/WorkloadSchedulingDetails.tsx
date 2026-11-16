import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { assertNever } from "core/utils/asserts";

import { BlockedWorkloads, WorkloadSchedulingDetailsData } from "./types";
import styles from "./WorkloadSchedulingDetails.module.scss";
import { Age } from "../Age";
import { LevelValue } from "../LevelValue";

const WORKLOAD_TYPE_MESSAGE_ID: Partial<Record<string, string>> = {
  SYNC: "settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.type.sync",
  CHECK: "settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.type.check",
  DISCOVER: "settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.type.discover",
};

// A workload type this page does not know is shown as the data plane reports it.
const WorkloadTypeName: React.FC<{ workloadType: string }> = ({ workloadType }) => {
  const messageId = WORKLOAD_TYPE_MESSAGE_ID[workloadType];

  return messageId ? <FormattedMessage id={messageId} /> : <>{workloadType}</>;
};

const DetailColumn: React.FC<React.PropsWithChildren<{ labelId: string }>> = ({ labelId, children }) => (
  <FlexContainer direction="column" gap="sm">
    <Text size="sm" color="grey400">
      <FormattedMessage id={labelId} />
    </Text>
    {children}
  </FlexContainer>
);

const BlockedValue: React.FC<{ blocked: BlockedWorkloads }> = ({ blocked }) => {
  switch (blocked.kind) {
    case "none":
      return (
        <LevelValue level="success">
          <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.none" />
        </LevelValue>
      );
    case "notMeasured":
      return (
        <LevelValue level="warning">
          <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.notMeasured" />
        </LevelValue>
      );
    case "blocked":
      return (
        <LevelValue level="error">
          <FormattedMessage
            id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.waiting"
            values={{
              type: <WorkloadTypeName workloadType={blocked.longest.workloadType} />,
              duration: <Age age={blocked.longest.waiting} />,
            }}
          />
        </LevelValue>
      );
    default:
      return assertNever(blocked);
  }
};

interface WorkloadSchedulingDetailsProps {
  data: WorkloadSchedulingDetailsData;
}

export const WorkloadSchedulingDetails: React.FC<WorkloadSchedulingDetailsProps> = ({
  data: { blocked, lastScheduled, lastProblem, error },
}) => (
  <FlexContainer direction="column" gap="sm">
    <Text size="sm" bold>
      <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.title" />
    </Text>

    <div className={styles.panel}>
      <DetailColumn labelId="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.blocked">
        <BlockedValue blocked={blocked} />
      </DetailColumn>

      <DetailColumn labelId="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.lastScheduled">
        {lastScheduled ? (
          <LevelValue level="success">
            <FormattedMessage
              id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.lastScheduledValue"
              values={{
                age: <Age age={lastScheduled.age} />,
                type: <WorkloadTypeName workloadType={lastScheduled.workloadType} />,
              }}
            />
          </LevelValue>
        ) : (
          <Text size="sm" bold>
            <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.empty" />
          </Text>
        )}
      </DetailColumn>

      <DetailColumn labelId="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.lastProblem">
        <Text size="sm" bold>
          {lastProblem ? (
            <FormattedMessage
              id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.lastProblemValue"
              values={{ age: <Age age={lastProblem.age} />, message: lastProblem.message }}
            />
          ) : (
            <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.noneRecorded" />
          )}
        </Text>
      </DetailColumn>
    </div>

    {error && (
      <Text size="sm" color="grey400">
        <FormattedMessage
          id="settings.organization.regions.dataplane.diagnostics.workloadScheduling.details.error"
          values={{ code: error.code, message: error.message }}
        />
      </Text>
    )}
  </FlexContainer>
);
