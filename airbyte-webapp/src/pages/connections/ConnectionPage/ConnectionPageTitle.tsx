import React, { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate, useParams } from "react-router-dom";

import { ConnectionInfoCard } from "components/connection/ConnectionInfoCard";
import { ConnectionName } from "components/connection/ConnectionName";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { StepsMenu } from "components/ui/StepsMenu";
import { Text } from "components/ui/Text";

import { ConnectionStatus } from "core/request/AirbyteClient";
import { useFeature, FeatureItem } from "core/services/features";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./ConnectionPageTitle.module.scss";
import { ConnectionRoutePaths } from "../types";

const InlineEnrollmentCallout = React.lazy(
  () => import("packages/cloud/components/experiments/FreeConnectorProgram/InlineEnrollmentCallout")
);

export const ConnectionPageTitle: React.FC = () => {
  const params = useParams<{ workspaceId: string; connectionId: string; "*": ConnectionRoutePaths }>();
  const navigate = useNavigate();
  const currentTab = params["*"] || ConnectionRoutePaths.Status;

  const { connection, schemaRefreshing } = useConnectionEditService();

  const streamCentricUIEnabled = useExperiment("connection.streamCentricUI.v1", false);
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);

  const tabs = useMemo(() => {
    const tabs = [
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
      // insert as the 2nd step
      tabs.splice(1, 0, {
        id: ConnectionRoutePaths.JobHistory,
        name: <FormattedMessage id="connectionForm.jobHistory" />,
      });
    }

    connection.status !== ConnectionStatus.deprecated &&
      tabs.push({
        id: ConnectionRoutePaths.Settings,
        name: <FormattedMessage id="sources.settings" />,
      });

    return tabs;
  }, [connection.status, streamCentricUIEnabled]);

  const onSelectTab = useCallback(
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
    <div className={isNewConnectionFlowEnabled ? styles.nextContainer : styles.container}>
      <div className={isNewConnectionFlowEnabled ? styles.container : undefined}>
        {connection.status === ConnectionStatus.deprecated && (
          <Message
            className={styles.connectionDeleted}
            type="warning"
            text={<FormattedMessage id="connection.connectionDeletedView" />}
          />
        )}
        <Text as="div" align="center" bold className={styles.connectionTitle}>
          <FormattedMessage id="connection.title" />
        </Text>
        <ConnectionName />
        <div className={styles.statusContainer}>
          <FlexContainer direction="column" gap="lg">
            <ConnectionInfoCard />
            {fcpEnabled && <InlineEnrollmentCallout />}
          </FlexContainer>
        </div>
      </div>
      <StepsMenu lightMode data={tabs} onSelect={onSelectTab} activeStep={currentTab} disabled={schemaRefreshing} />
    </div>
  );
};
