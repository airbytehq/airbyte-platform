import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { useGetConnectionEvent } from "core/api";
import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";

export const JobLogsModalContent: React.FC<{
  eventId?: string;
  jobId?: number;
  attemptNumber?: number;
  resetFilters?: () => void;
  connection: WebBackendConnectionRead;
}> = ({ eventId, jobId, attemptNumber, resetFilters, connection }) => {
  const { data: singleEventItem } = useGetConnectionEvent(eventId ?? null, connection.connectionId);

  const jobIdFromEvent = singleEventItem?.summary.jobId;

  const jobIdToUse = jobId ?? jobIdFromEvent;

  if (!jobIdToUse) {
    if (!!resetFilters) {
      resetFilters();
    }
    return null;
  }

  return <JobLogsModal jobId={jobIdToUse} initialAttemptId={attemptNumber} eventId={eventId} connection={connection} />;
};
