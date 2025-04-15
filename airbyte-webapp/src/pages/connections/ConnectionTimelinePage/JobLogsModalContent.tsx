import { z } from "zod";

import { JobLogsModal } from "area/connection/components/JobLogsModal/JobLogsModal";
import { useGetConnectionEvent } from "core/api";
import { WebBackendConnectionRead } from "core/api/types/AirbyteClient";

const EventSummarySchema = z
  .object({
    jobId: z.number().optional(),
  })
  .catchall(z.unknown());

export const extractJobId = (summary: Record<string, unknown>) => {
  const parseResult = EventSummarySchema.safeParse(summary);

  if (!parseResult.success) {
    return undefined;
  }

  return parseResult.data.jobId;
};

export const JobLogsModalContent: React.FC<{
  eventId?: string;
  jobId?: number;
  attemptNumber?: number;
  resetFilters?: () => void;
  connection: WebBackendConnectionRead;
}> = ({ eventId, jobId, attemptNumber, resetFilters, connection }) => {
  const { data: singleEventItem } = useGetConnectionEvent(eventId ?? null, connection.connectionId);

  const jobIdFromEvent = singleEventItem?.summary ? extractJobId(singleEventItem?.summary) : undefined;

  const jobIdToUse = jobId ?? jobIdFromEvent;

  if (!jobIdToUse) {
    if (!!resetFilters) {
      resetFilters();
    }
    return null;
  }

  return <JobLogsModal jobId={jobIdToUse} initialAttemptId={attemptNumber} eventId={eventId} connection={connection} />;
};
