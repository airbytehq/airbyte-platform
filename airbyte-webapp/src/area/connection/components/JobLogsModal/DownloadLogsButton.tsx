import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

interface DownloadButtonProps {
  downloadLogs: () => void;
}

export const DownloadLogsButton: React.FC<DownloadButtonProps> = ({ downloadLogs }) => {
  const { formatMessage } = useIntl();

  return (
    <Tooltip
      control={
        <Button
          onClick={downloadLogs}
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
