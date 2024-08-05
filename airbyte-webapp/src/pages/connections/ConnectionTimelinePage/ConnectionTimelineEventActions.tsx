import { FormattedDate } from "react-intl";

import { Text } from "components/ui/Text";

import styles from "./ConnectionTimelineEventActions.module.scss";
import { JobEventMenu } from "./JobEventMenu";

interface ConnectionTimelineEventActionsProps {
  jobId: number;
  eventId?: string;
  createdAt: number;
}

export const ConnectionTimelineEventActions: React.FC<ConnectionTimelineEventActionsProps> = ({
  jobId,
  eventId,
  createdAt,
}) => {
  return (
    <div className={styles.eventActions}>
      <Text color="grey400">
        <FormattedDate value={createdAt * 1000} timeStyle="short" dateStyle="medium" />
      </Text>
      <JobEventMenu jobId={jobId} eventId={eventId} />
    </div>
  );
};
