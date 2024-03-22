import classNames from "classnames";
import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useNavigate } from "react-router-dom";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { StreamWithStatus } from "components/connection/StreamStatus/streamStatusUtils";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Text } from "components/ui/Text";

import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectionRoutePaths } from "pages/routePaths";

import styles from "./StreamActionsMenu.module.scss";

interface StreamActionsMenuProps {
  streamState: StreamWithStatus;
}

export const StreamActionsMenu: React.FC<StreamActionsMenuProps> = ({ streamState }) => {
  const { formatMessage } = useIntl();
  const navigate = useNavigate();
  const sayClearInsteadOfReset = useExperiment("connection.clearNotReset", false);
  const { syncStarting, jobSyncRunning, resetStarting, jobResetRunning, resetStreams } = useConnectionSyncContext();
  const { mode } = useConnectionFormService();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const options: DropdownMenuOptionType[] = [
    ...(sayClearInsteadOfReset
      ? []
      : [
          {
            displayName: formatMessage({ id: "connection.stream.actions.resetThisStream" }),
            value: "resetThisStream",
            disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning || mode === "readonly",
          },
        ]),
    {
      displayName: formatMessage({ id: "connection.stream.actions.showInReplicationTable" }),
      value: "showInReplicationTable",
    },
    {
      displayName: formatMessage({ id: "connection.stream.actions.openDetails" }),
      value: "openDetails",
    },
    ...(!sayClearInsteadOfReset
      ? []
      : [
          {
            displayName: formatMessage({
              id: "connection.stream.actions.clearData",
            }),
            value: "clearStreamData",
            disabled: syncStarting || jobSyncRunning || resetStarting || jobResetRunning || mode === "readonly",
            className: classNames(styles.streamActionsMenu__clearDataLabel),
          },
        ]),
  ];

  const onOptionClick = async ({ value }: DropdownMenuOptionType) => {
    if (value === "showInReplicationTable" || value === "openDetails") {
      navigate(`../${ConnectionRoutePaths.Replication}`, {
        state: { namespace: streamState?.streamNamespace, streamName: streamState?.streamName, action: value },
      });
    }

    if (value === "clearStreamData") {
      openConfirmationModal({
        title: (
          <FormattedMessage
            id="connection.stream.actions.clearData.confirm.title"
            values={{
              streamName: (
                <span className={styles.streamActionsMenu__clearDataModalStreamName}>{streamState.streamName}</span>
              ),
            }}
          />
        ),
        text: "connection.stream.actions.clearData.confirm.text",
        additionalContent: (
          <Box pt="xl">
            <Text color="grey400">
              <FormattedMessage id="connection.stream.actions.clearData.confirm.additionalText" />
            </Text>
          </Box>
        ),
        submitButtonText: "connection.stream.actions.clearData.confirm.submit",
        cancelButtonText: "connection.stream.actions.clearData.confirm.cancel",
        onSubmit: async () => {
          await resetStreams([{ streamNamespace: streamState.streamNamespace, streamName: streamState.streamName }]);
          closeConfirmationModal();
        },
      });
    }
    if (value === "resetThisStream" && streamState) {
      await resetStreams([{ streamNamespace: streamState.streamNamespace, streamName: streamState.streamName }]);
    }
  };

  return (
    <DropdownMenu placement="bottom-end" options={options} onChange={onOptionClick}>
      {() => <Button variant="clear" icon="options" />}
    </DropdownMenu>
  );
};
