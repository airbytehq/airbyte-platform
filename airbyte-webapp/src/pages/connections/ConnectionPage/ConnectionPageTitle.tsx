import { faTrash } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import React, { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { ConnectionInfoCard } from "components/connection/ConnectionInfoCard";
import { ConnectionName } from "components/connection/ConnectionName";
import { Callout } from "components/ui/Callout";
import { FlexContainer } from "components/ui/Flex";
import { StepsMenu } from "components/ui/StepsMenu";
import { Text } from "components/ui/Text";

import { ConnectionStatus } from "core/request/AirbyteClient";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";
import { useFeature, FeatureItem } from "hooks/services/Feature";

import styles from "./ConnectionPageTitle.module.scss";
import { ConnectionRoutePaths } from "../types";

const LargeEnrollmentCallout = React.lazy(
  () => import("packages/cloud/components/experiments/FreeConnectorProgram/LargeEnrollmentCallout")
);

export const ConnectionPageTitle: React.FC = () => {
  const params = useParams<{ id: string; "*": ConnectionRoutePaths }>();
  const navigate = useNavigate();
  const currentStep = params["*"] || ConnectionRoutePaths.Status;

  const { connection, schemaRefreshing } = useConnectionEditService();

  const streamCentricUIEnabled = useExperiment("connection.streamCentricUI.v1", false);

  const steps = useMemo(() => {
    const steps = [
      {
        id: ConnectionRoutePaths.Status,
        name: <FormattedMessage id="sources.status" />,
      },
      {
        id: ConnectionRoutePaths.Replication,
        name: <FormattedMessage id="connection.replication" />,
      },
      {
        id: ConnectionRoutePaths.Transformation,
        name: <FormattedMessage id="connectionForm.transformation.title" />,
      },
    ];

    if (streamCentricUIEnabled) {
      steps.push({
        id: ConnectionRoutePaths.JobHistory,
        name: <FormattedMessage id="connectionForm.jobHistory" />,
      });
    }

    connection.status !== ConnectionStatus.deprecated &&
      steps.push({
        id: ConnectionRoutePaths.Settings,
        name: <FormattedMessage id="sources.settings" />,
      });

    return steps;
  }, [connection.status, streamCentricUIEnabled]);

  const onSelectStep = useCallback(
    (id: string) => {
      if (id === ConnectionRoutePaths.Status) {
        navigate("");
      } else {
        navigate(id);
      }
    },
    [navigate]
  );

  const fcpEnabled = useFeature(FeatureItem.FreeConnectorProgram);

  return (
    <div className={styles.container}>
      {connection.status === ConnectionStatus.deprecated && (
        <Callout className={styles.connectionDeleted}>
          <FontAwesomeIcon icon={faTrash} size="lg" />
          <FormattedMessage id="connection.connectionDeletedView" />
        </Callout>
      )}
      <Text as="div" centered bold className={styles.connectionTitle}>
        <FormattedMessage id="connection.title" />
      </Text>
      <ConnectionName />
      <div className={styles.statusContainer}>
        <FlexContainer direction="column" gap="none">
          <ConnectionInfoCard />
          {fcpEnabled && <LargeEnrollmentCallout />}
        </FlexContainer>
      </div>
      <StepsMenu lightMode data={steps} onSelect={onSelectStep} activeStep={currentStep} disabled={schemaRefreshing} />
    </div>
  );
};
