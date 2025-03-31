import { FormattedDate, FormattedMessage } from "react-intl";
import { z } from "zod";

import { Badge } from "components/ui/Badge";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Markdown } from "components/ui/Markdown";
import { Text } from "components/ui/Text";

import { useExplainJob } from "core/api";
import { JobStats } from "pages/connections/ConnectionTimelinePage/components/JobStats";
import { ConnectionTimelineEventIcon } from "pages/connections/ConnectionTimelinePage/ConnectionTimelineEventIcon";
import { ConnectionTimelineEventSummary } from "pages/connections/ConnectionTimelinePage/ConnectionTimelineEventSummary";
import { jobSummarySchema } from "pages/connections/ConnectionTimelinePage/types";
import { getStatusByEventType, getStatusIcon } from "pages/connections/ConnectionTimelinePage/utils";

import styles from "./AISyncFailureExplanation.module.scss";

interface AISyncFailureExplanationProps {
  jobSummary: z.infer<typeof jobSummarySchema>;
  jobStatus: ReturnType<typeof getStatusByEventType>;
  titleId: string;
  titleValues?: Record<string, string | number>;
}

export const AISyncFailureDrawerTitle = () => {
  return (
    <Box px="lg">
      <FlexContainer alignItems="center" gap="sm">
        <Heading as="h2">
          <FormattedMessage id="connection.llmSyncFailureExplanation.title" />
        </Heading>
        <Badge variant="blue">
          <FormattedMessage id="ui.badge.beta" />
        </Badge>
      </FlexContainer>
    </Box>
  );
};

export const AISyncFailureExplanation: React.FC<AISyncFailureExplanationProps> = ({
  jobStatus,
  jobSummary,
  titleId,
  titleValues,
}) => {
  const { data, isLoading, isError } = useExplainJob(jobSummary.jobId);

  return (
    <Box px="lg" data-testid="ai-sync-failure-explanation-container">
      <Box py="lg">
        <FlexContainer justifyContent="space-between" alignItems="center">
          <FlexContainer alignItems="center" gap="lg">
            <ConnectionTimelineEventIcon icon="sync" statusIcon={getStatusIcon(jobStatus)} />
            <ConnectionTimelineEventSummary>
              <Text bold>
                <FormattedMessage id={titleId} values={titleValues} />
              </Text>
              <JobStats summary={jobSummary} />
            </ConnectionTimelineEventSummary>
          </FlexContainer>
          {jobSummary.endTimeEpochSeconds && (
            <Text color="grey400">
              <FormattedDate value={jobSummary.endTimeEpochSeconds * 1000} timeStyle="short" dateStyle="medium" />
            </Text>
          )}
        </FlexContainer>
      </Box>
      {isError && (
        <Text size="lg">
          <FormattedMessage id="connection.llmSyncFailureExplanation.error" />
        </Text>
      )}
      {!isError && isLoading && !data && (
        <Text size="lg">
          <FormattedMessage id="connection.llmSyncFailureExplanation.loading" />
        </Text>
      )}
      {data?.explanation && (
        <Markdown
          data-testid="ai-sync-failure-explanation"
          className={styles.syncFailureExplanation}
          content={data.explanation}
        />
      )}
    </Box>
  );
};
