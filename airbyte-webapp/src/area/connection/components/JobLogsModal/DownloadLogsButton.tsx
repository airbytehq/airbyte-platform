import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { CleanedLogLines } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { useCurrentWorkspace } from "core/api";
import { downloadFile, FILE_TYPE_DOWNLOAD, fileizeString } from "core/utils/file";

interface DownloadButtonProps {
  logLines: CleanedLogLines;
  fileName: string;
}

export const DownloadLogsButton: React.FC<DownloadButtonProps> = ({ logLines, fileName }) => {
  const { formatMessage } = useIntl();
  const { name } = useCurrentWorkspace();

  const downloadFileWithLogs = () => {
    const file = new Blob([logLines.map((logLine) => logLine.text).join("\n")], {
      type: FILE_TYPE_DOWNLOAD,
    });
    downloadFile(file, fileizeString(`${name}-${fileName}.txt`));
  };

  return (
    <Tooltip
      control={
        <Button
          onClick={downloadFileWithLogs}
          variant="secondary"
          icon="download"
          aria-label={formatMessage({ id: "jobHistory.logs.downloadLogs" })}
        />
      }
      placement="bottom"
    >
      <FormattedMessage id="jobHistory.logs.downloadLogs" />
    </Tooltip>
  );
};
