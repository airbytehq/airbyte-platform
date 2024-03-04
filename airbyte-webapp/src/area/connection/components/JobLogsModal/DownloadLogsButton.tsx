import React from "react";
import { useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";

import { CleanedLogLines } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { useCurrentWorkspace } from "core/api";
import { FILE_TYPE_DOWNLOAD, downloadFile, fileizeString } from "core/utils/file";

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
    <Button
      onClick={downloadFileWithLogs}
      variant="secondary"
      title={formatMessage({
        id: "jobHistory.logs.downloadLogs",
      })}
      icon={<Icon type="download" />}
    />
  );
};
