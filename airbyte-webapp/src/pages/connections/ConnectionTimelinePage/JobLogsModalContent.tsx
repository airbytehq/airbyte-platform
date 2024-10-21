import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { useGetConnectionEvent } from "core/api";

export const JobLogsModalContent: React.FC<{
  eventId?: string;
  jobId?: number;
  attemptNumber?: number;
  resetFilters?: () => void;
  connectionId: string;
}> = ({ eventId, jobId, attemptNumber, resetFilters, connectionId }) => {
  const { data: singleEventItem } = useGetConnectionEvent(eventId ?? null, connectionId);

  const jobIdFromEvent = singleEventItem?.summary.jobId;

  const jobIdToUse = jobId ?? jobIdFromEvent;

  if (!jobIdToUse) {
    if (!!resetFilters) {
      resetFilters();
    }
    return null;
  }

  return (
    <JobLogsModal jobId={jobIdToUse} initialAttemptId={attemptNumber} eventId={eventId} connectionId={connectionId} />
  );
};
