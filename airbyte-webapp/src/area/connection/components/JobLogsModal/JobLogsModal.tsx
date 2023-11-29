import { useEffect, useMemo, useRef, useState } from "react";
import { useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";

import { AttemptDetails } from "area/connection/components/AttemptDetails";
import { LogSearchInput } from "area/connection/components/JobHistoryItem/LogSearchInput";
import { useCleanLogs } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { VirtualLogs } from "area/connection/components/JobHistoryItem/VirtualLogs";
import { LinkToAttemptButton } from "area/connection/components/JobLogsModal/LinkToAttemptButton";
import { useAttemptForJob, useJobInfoWithoutLogs } from "core/api";

import { AttemptStatusIcon } from "./AttemptStatusIcon";
import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModal.module.scss";
import { JobLogsModalFailureMessage } from "./JobLogsModalFailureMessage";

interface JobLogsModalProps {
  jobId: number;
  initialAttemptId?: number;
}

export const JobLogsModal: React.FC<JobLogsModalProps> = ({ jobId, initialAttemptId }) => {
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [inputValue, setInputValue] = useState("");
  const job = useJobInfoWithoutLogs(jobId);
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;
  const [selectedAttemptId, setSelectedAttemptId] = useState(
    initialAttemptId ?? job.attempts[job.attempts.length - 1].attempt.id
  );
  const jobAttempt = useAttemptForJob(jobId, selectedAttemptId);
  const logLines = useCleanLogs(jobAttempt);
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

  // Debounces changes to the search input so we don't recompute the matching lines on every keystroke
  useDebounce(
    () => {
      setDebouncedSearchTerm(inputValue);
      setHighlightedMatchIndex(undefined);
      const searchTermLowerCase = inputValue.toLowerCase();
      if (inputValue.length > 0) {
        const matchingLines: number[] = [];
        logLines.forEach(
          (line, index) => line.text.toLocaleLowerCase().includes(searchTermLowerCase) && matchingLines.push(index)
        );
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
    [inputValue, logLines]
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
  };

  // Focus the search input with cmd + f / ctrl + f
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "f" && (navigator.platform.toLowerCase().includes("mac") ? e.metaKey : e.ctrlKey)) {
        e.preventDefault();
        searchInputRef.current?.focus();
      }
    };
    document.body.addEventListener("keydown", handleKeyDown);
    return () => document.body.removeEventListener("keydown", handleKeyDown);
  }, []);

  return (
    <FlexContainer direction="column" style={{ height: "100%" }}>
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
          <AttemptDetails attempt={jobAttempt.attempt} jobId={String(jobId)} showEndedAt showFailureMessage={false} />
          <FlexContainer className={styles.downloadLogs}>
            <LinkToAttemptButton jobId={jobId} attemptId={selectedAttemptId} />
            <DownloadLogsButton logLines={logLines} fileName={`job-${jobId}-attempt-${selectedAttemptId + 1}`} />
          </FlexContainer>
        </FlexContainer>
      </Box>
      <Box px="md">
        <FlexContainer alignItems="center">
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
        </FlexContainer>
      </Box>
      <JobLogsModalFailureMessage failureSummary={jobAttempt.attempt.failureSummary} />
      <VirtualLogs
        selectedAttempt={selectedAttemptId}
        logLines={logLines}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
        hasFailure={!!jobAttempt.attempt.failureSummary}
      />
    </FlexContainer>
  );
};
