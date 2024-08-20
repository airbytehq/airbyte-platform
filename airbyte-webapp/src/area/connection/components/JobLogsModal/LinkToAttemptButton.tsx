import React, { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Button } from "components/ui/Button";
import { Tooltip } from "components/ui/Tooltip";

import { buildAttemptLink } from "area/connection/utils/attemptLink";
import { copyToClipboard } from "core/utils/clipboard";

interface Props {
  jobId: string | number;
  attemptId?: number;
  eventId?: string;
  openedFromTimeline?: boolean;
}

export const LinkToAttemptButton: React.FC<Props> = ({ jobId, attemptId, eventId, openedFromTimeline }) => {
  const { formatMessage } = useIntl();
  const [showCopiedTooltip, setShowCopiedTooltip] = useState(false);
  const [hideTooltip] = useDebounce(() => setShowCopiedTooltip(false), 3000, [showCopiedTooltip]);
  const onCopyLink = async () => {
    const url = new URL(window.location.href);

    if (openedFromTimeline) {
      url.searchParams.set("openLogs", "true");
      if (eventId) {
        url.searchParams.set("eventId", eventId);
      } else {
        url.searchParams.set("jobId", jobId.toString());
      }
      if (attemptId) {
        url.searchParams.set("attemptId", attemptId.toString());
      }
    } else {
      url.hash = buildAttemptLink(jobId, attemptId);
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
