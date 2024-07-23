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
}

export const LinkToAttemptButton: React.FC<Props> = ({ jobId, attemptId }) => {
  const { formatMessage } = useIntl();

  const [showCopiedTooltip, setShowCopiedTooltip] = useState(false);
  const [hideTooltip] = useDebounce(() => setShowCopiedTooltip(false), 3000, [showCopiedTooltip]);

  const onCopyLink = async () => {
    // Get the current URL and replace (or add) hash to current log
    const url = new URL(window.location.href);
    url.hash = buildAttemptLink(jobId, attemptId);
    await copyToClipboard(url.href);
    // Show and hide tooltip with a delay again
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
