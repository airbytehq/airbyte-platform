import { CellContext } from "@tanstack/react-table";
import { FormattedMessage } from "react-intl";

import { ConnectionStatusIndicatorStatus } from "components/connection/ConnectionStatusIndicator";
import { StreamStatusIndicator } from "components/connection/StreamStatusIndicator";
import { FlexContainer } from "components/ui/Flex";

import { UIStreamState } from "area/connection/utils/useUiStreamsStates";
import { useCurrentTime, useFormatLengthOfTime } from "core/utils/time";

import styles from "./StreamsList.module.scss";

export const StatusCell: React.FC<CellContext<UIStreamState, ConnectionStatusIndicatorStatus>> = (props) => {
  const now = useCurrentTime();
  let isRateLimited = props.row.original.status === ConnectionStatusIndicatorStatus.RateLimited;
  let rateLimitTimeRemaining;
  if (
    props.row.original.status === ConnectionStatusIndicatorStatus.RateLimited &&
    props.row.original.quotaReset != null
  ) {
    if (props.row.original.quotaReset < now) {
      // quota reset time has passed, no longer display rate limited status
      isRateLimited = false;
    } else {
      rateLimitTimeRemaining = props.row.original.quotaReset - now;
    }
  }
  const rateLimitedMessage = useFormatLengthOfTime(rateLimitTimeRemaining);

  // detect the hour/minute/second segments in the string we will render
  // and for each segment, increase the width by 30px (in addition to a base 60px for "rendering ")
  const rateLimitTextParts = rateLimitedMessage.match(/(?<h>\d+h )?(?<m>\d+m )?(?<s>\d+s)/);
  const rateLimitTextWidth =
    60 + Object.entries(rateLimitTextParts?.groups ?? {}).filter(([, value]) => Boolean(value)).length * 30;
  return (
    <FlexContainer justifyContent="flex-start" gap="sm" alignItems="center" className={styles.statusCell}>
      <StreamStatusIndicator status={props.cell.getValue()} />
      <FormattedMessage id={`connection.stream.status.${props.cell.getValue()}`} />
      {isRateLimited && (
        <>
          &nbsp;
          <FormattedMessage id="general.dash" />
          <span style={{ width: `${rateLimitTextWidth}px` }}>
            <FormattedMessage
              id={
                rateLimitTimeRemaining
                  ? "connection.stream.rateLimitedMessage"
                  : "connection.stream.rateLimitedMessageNoQuota"
              }
              values={{ quota: rateLimitedMessage }}
            />
          </span>
        </>
      )}
    </FlexContainer>
  );
};
