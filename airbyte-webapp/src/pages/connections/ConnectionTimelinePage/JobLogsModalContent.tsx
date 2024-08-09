import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { useGetConnectionEvent } from "core/api";

export const JobLogsModalContent: React.FC<{
  eventId?: string;
  jobId?: number;
  attemptNumber?: number;
  resetFilters?: () => void;
}> = ({ eventId, jobId, attemptNumber, resetFilters }) => {
  const { data: singleEventItem } = useGetConnectionEvent(eventId ?? null);

  const jobIdFromEvent = singleEventItem?.summary.jobId;

  const jobIdToUse = jobId ?? jobIdFromEvent;

  if (!jobIdToUse) {
    if (!!resetFilters) {
      resetFilters();
    }
    return null;
  }

  return <JobLogsModal jobId={jobIdToUse} initialAttemptId={attemptNumber} eventId={eventId} openedFromTimeline />;
};
