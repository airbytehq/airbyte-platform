import React from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useAsyncFn } from "react-use";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useGetLogs } from "core/api";
import { LogType } from "core/api/types/AirbyteClient";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { downloadFile } from "core/utils/file";
import { useNotificationService } from "hooks/services/Notification";

const LogsContent: React.FC = () => {
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const analyticsService = useAnalyticsService();

  const fetchLogs = useGetLogs();

  const downloadLogs = async (logType: LogType) => {
    try {
      const file = await fetchLogs({ logType });
      const name = `${logType}-logs.txt`;
      downloadFile(file, name);
    } catch (e) {
      console.error(e);

      registerNotification({
        id: "admin.logs.error",
        text: formatMessage({ id: "admin.logs.error" }),
        type: "error",
      });
    }
  };

  // TODO: get rid of useAsyncFn and use react-query
  const [{ loading: serverLogsLoading }, downloadServerLogs] = useAsyncFn(async () => {
    analyticsService.track(Namespace.SETTINGS, Action.DOWNLOAD_SERVER_LOGS, {});
    await downloadLogs(LogType.server);
  }, [downloadLogs, analyticsService]);

  const [{ loading: schedulerLogsLoading }, downloadSchedulerLogs] = useAsyncFn(async () => {
    analyticsService.track(Namespace.SETTINGS, Action.DOWNLOAD_SCHEDULER_LOGS, {});
    await downloadLogs(LogType.scheduler);
  }, [downloadLogs, analyticsService]);

  return (
    <Box p="xl">
      <FlexContainer>
        <Button onClick={downloadServerLogs} isLoading={serverLogsLoading}>
          <FormattedMessage id="admin.downloadServerLogs" />
        </Button>
        <Button onClick={downloadSchedulerLogs} isLoading={schedulerLogsLoading}>
          <FormattedMessage id="admin.downloadSchedulerLogs" />
        </Button>
      </FlexContainer>
    </Box>
  );
};

export default LogsContent;
