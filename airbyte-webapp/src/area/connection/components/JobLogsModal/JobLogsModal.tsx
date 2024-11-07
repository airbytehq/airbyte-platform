import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { MultiListBox } from "components/ui/ListBox/MultiListBox";
import { Message } from "components/ui/Message";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { AttemptDetails } from "area/connection/components/AttemptDetails";
import { LogSearchInput } from "area/connection/components/JobHistoryItem/LogSearchInput";
import { JobLogOrigins, KNOWN_LOG_ORIGINS, useCleanLogs } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { VirtualLogs } from "area/connection/components/JobHistoryItem/VirtualLogs";
import { LinkToAttemptButton } from "area/connection/components/JobLogsModal/LinkToAttemptButton";
import {
  attemptHasStructuredLogs,
  useAttemptCombinedStatsForJob,
  useAttemptForJob,
  useJobInfoWithoutLogs,
} from "core/api";
import { trackError } from "core/utils/datadog";
import { useExperiment } from "hooks/services/Experiment";

import { AttemptStatusIcon } from "./AttemptStatusIcon";
import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModal.module.scss";
import { JobLogsModalFailureMessage } from "./JobLogsModalFailureMessage";

interface JobLogsModalProps {
  connectionId: string;
  jobId: number;
  initialAttemptId?: number;
  eventId?: string;
}

export const JobLogsModal: React.FC<JobLogsModalProps> = ({ jobId, initialAttemptId, eventId, connectionId }) => {
  const job = useJobInfoWithoutLogs(jobId);

  if (job.attempts.length === 0) {
    trackError(new Error(`No attempts for job`), { jobId, eventId });

    return (
      <Box p="lg">
        <Message type="warning" text={<FormattedMessage id="jobHistory.logs.noAttempts" />} />
      </Box>
    );
  }

  return (
    <JobLogsModalInner
      jobId={jobId}
      initialAttemptId={initialAttemptId}
      eventId={eventId}
      connectionId={connectionId}
    />
  );
};

const JobLogsModalInner: React.FC<JobLogsModalProps> = ({ jobId, initialAttemptId, eventId, connectionId }) => {
  const searchInputRef = useRef<HTMLInputElement>(null);
  const job = useJobInfoWithoutLogs(jobId);
  const showStructuredLogsUI = useExperiment("logs.structured-logs-ui");

  const [inputValue, setInputValue] = useState("");
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;

  const [selectedAttemptId, setSelectedAttemptId] = useState(
    initialAttemptId ?? job.attempts[job.attempts.length - 1].attempt.id
  );

  const jobAttempt = useAttemptForJob(jobId, selectedAttemptId);
  const aggregatedAttemptStats = useAttemptCombinedStatsForJob(jobId, selectedAttemptId, {
    refetchInterval() {
      // if the attempt hasn't ended refetch every 2.5 seconds
      return jobAttempt.attempt.endedAt ? false : 2500;
    },
  });
  const { logLines, origins } = useCleanLogs(jobAttempt);
  const [selectedLogOrigins, setSelectedLogOrigins] = useState<JobLogOrigins[] | null>(
    KNOWN_LOG_ORIGINS.map(({ key }) => key)
  );
  const firstMatchIndex = 0;
  const lastMatchIndex = matchingLines.length - 1;
  const [debouncedSearchTerm, setDebouncedSearchTerm] = useState("");
  const scrollTo = useMemo(
    () => (matchingLines && highlightedMatchIndex !== undefined ? matchingLines[highlightedMatchIndex] : undefined),
    [matchingLines, highlightedMatchIndex]
  );
  const { formatMessage } = useIntl();

  const attemptListboxOptions = useMemo(() => {
    return job.attempts.map((attempt, index) => ({
      label: formatMessage(
        { id: "jobHistory.logs.attemptLabel" },
        { attemptNumber: index + 1, totalAttempts: job.attempts.length }
      ),
      value: attempt.attempt.id,
      icon: <AttemptStatusIcon attempt={attempt} />,
    }));
  }, [job, formatMessage]);

  const onSelectAttempt = (selectedAttemptId: number) => {
    setSelectedAttemptId(selectedAttemptId);
    setHighlightedMatchIndex(undefined);
    setMatchingLines([]);
    setInputValue("");
  };

  const logOriginOptions = useMemo<Array<{ label: string; value: JobLogOrigins }>>(
    () =>
      KNOWN_LOG_ORIGINS.map(({ key }) => {
        return { label: formatMessage({ id: `jobHistory.logs.logOrigin.${key}` }), value: key };
      }),
    [formatMessage]
  );

  const onSelectLogOrigin = useCallback(
    (origin: JobLogOrigins) => {
      if (!selectedLogOrigins) {
        setSelectedLogOrigins(origins.filter((o) => o !== origin));
      } else {
        setSelectedLogOrigins(
          selectedLogOrigins.includes(origin)
            ? selectedLogOrigins.filter((o) => o !== origin)
            : [...selectedLogOrigins, origin]
        );
      }
    },
    [origins, selectedLogOrigins]
  );

  const filteredLogLines = useMemo(() => {
    return logLines.filter((line) => selectedLogOrigins?.includes(line.domain ?? JobLogOrigins.Other) ?? true);
  }, [logLines, selectedLogOrigins]);

  // Debounces changes to the search input so we don't recompute the matching lines on every keystroke
  useDebounce(
    () => {
      setDebouncedSearchTerm(inputValue);
      setHighlightedMatchIndex(undefined);
      const searchTermLowerCase = inputValue.toLowerCase();
      if (inputValue.length > 0) {
        const matchingLines: number[] = [];
        filteredLogLines.forEach((line, index) => {
          return line.text.toLocaleLowerCase().includes(searchTermLowerCase) && matchingLines.push(index);
        });
        setMatchingLines(matchingLines);
        if (matchingLines.length > 0) {
          setHighlightedMatchIndex(firstMatchIndex);
        } else {
          setHighlightedMatchIndex(undefined);
        }
      } else {
        setMatchingLines([]);
        setHighlightedMatchIndex(undefined);
      }
    },
    150,
    [inputValue, filteredLogLines]
  );

  const onSearchTermChange = (searchTerm: string) => {
    setInputValue(searchTerm);
  };

  const onSearchInputKeydown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.shiftKey && e.key === "Enter") {
      e.preventDefault();
      scrollToPreviousMatch();
    } else if (e.key === "Enter") {
      e.preventDefault();
      scrollToNextMatch();
    }
  };

  const scrollToPreviousMatch = () => {
    if (matchingLines.length === 0) {
      return;
    }
    if (highlightedMatchIndex === undefined) {
      setHighlightedMatchIndex(lastMatchIndex);
    } else {
      setHighlightedMatchIndex(highlightedMatchIndex === firstMatchIndex ? lastMatchIndex : highlightedMatchIndex - 1);
    }
    searchInputRef.current?.focus();
  };

  const scrollToNextMatch = () => {
    if (matchingLines.length === 0) {
      return;
    }
    if (highlightedMatchIndex === undefined) {
      setHighlightedMatchIndex(firstMatchIndex);
    } else {
      setHighlightedMatchIndex(highlightedMatchIndex === lastMatchIndex ? firstMatchIndex : highlightedMatchIndex + 1);
    }
    searchInputRef.current?.focus();
  };

  // Focus the search input with cmd + f / ctrl + f
  // Clear search input on `esc`, if search input is focused
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "f" && (navigator.platform.toLowerCase().includes("mac") ? e.metaKey : e.ctrlKey)) {
        e.preventDefault();
        searchInputRef.current?.focus();
      } else if (e.key === "Escape" && document.activeElement === searchInputRef.current) {
        if (inputValue.length > 0) {
          e.preventDefault();
          setInputValue("");
        }
      }
    };
    document.body.addEventListener("keydown", handleKeyDown);
    return () => document.body.removeEventListener("keydown", handleKeyDown);
  }, [inputValue]);

  return (
    <FlexContainer direction="column" style={{ height: "100%" }} data-testid="job-logs-modal">
      <Box p="md" pb="none">
        <FlexContainer alignItems="center">
          <div className={styles.attemptDropdown}>
            <ListBox
              className={styles.attemptDropdown__listbox}
              selectedValue={selectedAttemptId}
              options={attemptListboxOptions}
              onSelect={onSelectAttempt}
              isDisabled={job.attempts.length === 1}
            />
          </div>
          <AttemptDetails
            attempt={jobAttempt.attempt}
            aggregatedAttemptStats={aggregatedAttemptStats}
            jobId={String(jobId)}
            showEndedAt
            showFailureMessage={false}
          />
          <FlexContainer className={styles.downloadLogs}>
            <LinkToAttemptButton
              connectionId={connectionId}
              jobId={jobId}
              attemptId={selectedAttemptId}
              eventId={eventId}
            />
            <DownloadLogsButton logLines={logLines} fileName={`job-${jobId}-attempt-${selectedAttemptId + 1}`} />
          </FlexContainer>
        </FlexContainer>
      </Box>
      <JobLogsModalFailureMessage failureSummary={jobAttempt.attempt.failureSummary} />
      <Box px="md">
        <FlexContainer>
          <FlexItem grow>
            <LogSearchInput
              ref={searchInputRef}
              inputValue={inputValue}
              onSearchInputKeydown={onSearchInputKeydown}
              onSearchTermChange={onSearchTermChange}
              highlightedMatchDisplay={highlightedMatchingLineNumber}
              highlightedMatchIndex={highlightedMatchIndex}
              matches={matchingLines}
              scrollToNextMatch={scrollToNextMatch}
              scrollToPreviousMatch={scrollToPreviousMatch}
            />
          </FlexItem>
          {showStructuredLogsUI && (
            <FlexItem>
              <MultiListBox
                selectedValues={selectedLogOrigins ?? origins}
                options={logOriginOptions}
                onSelectValues={(newOrigins) => setSelectedLogOrigins(newOrigins ?? origins)}
                label="Log sources"
              />
            </FlexItem>
          )}
        </FlexContainer>
      </Box>

      {origins.length > 0 && (
        <Box px="md">
          <FlexContainer gap="lg">
            {logOriginOptions.map((option) => (
              <label key={option.value}>
                <FlexContainer key={option.value} alignItems="center" as="span" display="inline-flex" gap="sm">
                  <Switch
                    size="xs"
                    checked={selectedLogOrigins?.includes(option.value) ?? true}
                    onChange={() => onSelectLogOrigin(option.value)}
                  />
                  <Text>{option.label}</Text>
                </FlexContainer>
              </label>
            ))}
          </FlexContainer>
        </Box>
      )}
      <VirtualLogs
        selectedAttempt={selectedAttemptId}
        logLines={filteredLogLines}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
        hasFailure={!!jobAttempt.attempt.failureSummary}
        attemptHasStructuredLogs={attemptHasStructuredLogs(jobAttempt)}
      />
    </FlexContainer>
  );
};
