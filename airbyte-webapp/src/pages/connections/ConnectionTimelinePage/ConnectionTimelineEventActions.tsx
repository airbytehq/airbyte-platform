import { FormattedDate } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ConnectionTimelineEventActions.module.scss";
import { JobEventMenu } from "./JobEventMenu";

interface ConnectionTimelineEventActionsProps {
  jobId?: number;
  eventId?: string;
  createdAt?: number;
  attemptCount?: number;
}

export const ConnectionTimelineEventActions: React.FC<ConnectionTimelineEventActionsProps> = ({
  jobId,
  eventId,
  createdAt,
  attemptCount,
}) => {
  return (
    <div className={styles.eventActions}>
      {createdAt && (
        <Text color="grey400">
          <FormattedDate value={createdAt * 1000} timeStyle="short" dateStyle="medium" />
        </Text>
      )}
      <div className={styles.eventActions__menu}>
        {jobId && <JobEventMenu jobId={jobId} eventId={eventId} attemptCount={attemptCount} />}
      </div>
    </div>
  );
};
