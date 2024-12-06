import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { copyToClipboard } from "core/utils/clipboard";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

interface Props {
  connectionId: string;
  jobId: string | number;
  attemptId?: number;
  eventId?: string;
}

export const LinkToAttemptButton: React.FC<Props> = ({ connectionId, jobId, attemptId, eventId }) => {
  const { formatMessage } = useIntl();
  const [showCopiedTooltip, setShowCopiedTooltip] = useState(false);
  const [hideTooltip] = useDebounce(() => setShowCopiedTooltip(false), 3000, [showCopiedTooltip]);
  const workspaceId = useCurrentWorkspaceId();

  const onCopyLink = async () => {
    const url = new URL(window.location.href);

    url.pathname = `${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Connections}/${connectionId}/${ConnectionRoutePaths.Timeline}`;
    url.searchParams.set("openLogs", "true");
    if (eventId) {
      url.searchParams.set("eventId", eventId);
    } else {
      url.searchParams.set("jobId", jobId.toString());
    }
    if (attemptId) {
      url.searchParams.set("attemptNumber", attemptId.toString());
    }

    await copyToClipboard(url.href);
    setShowCopiedTooltip(true);
    hideTooltip();
  };

  return (
    <Tooltip
      control={
        <Button
          variant="secondary"
          onClick={onCopyLink}
          aria-label={formatMessage({ id: "connection.copyLogLink" })}
          icon="link"
          data-testid="copy-link-to-attempt-button"
        />
      }
    >
      {showCopiedTooltip ? (
        <FormattedMessage id="connection.linkCopied" />
      ) : (
        <FormattedMessage id="connection.copyLogLink" />
      )}
    </Tooltip>
  );
};
