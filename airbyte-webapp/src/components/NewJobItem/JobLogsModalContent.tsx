import { useEffect, useMemo, useRef, useState } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useAttemptForJob, useJobInfoWithoutLogs } from "core/api";
import { formatBytes } from "core/utils/numberHelper";

import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModalContent.module.scss";
import { JobLogsModalFailureMessage } from "./JobLogsModalFailureMessage";
import { AttemptStatusIcon } from "./JobStatusIcon";
import { LogSearchInput } from "./LogSearchInput";
import { useCleanLogs } from "./useCleanLogs";
import { VirtualLogs } from "./VirtualLogs";

interface JobLogsModalContentProps {
  jobId: number;
}

export const JobLogsModalContent: React.FC<JobLogsModalContentProps> = ({ jobId }) => {
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [inputValue, setInputValue] = useState("");
  const job = useJobInfoWithoutLogs(jobId);
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;
  const [selectedAttemptIndex, setSelectedAttemptIndex] = useState(job.attempts.length - 1);
  const jobAttempt = useAttemptForJob(jobId, selectedAttemptIndex);
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
    return job.attempts.map((_, index) => ({
      label: formatMessage(
        { id: "jobHistory.logs.attemptLabel" },
        { attemptNumber: index + 1, totalAttempts: job.attempts.length }
      ),
      value: index,
      icon: <AttemptStatusIcon attempt={job.attempts[index]} />,
    }));
  }, [job, formatMessage]);

  const onSelectAttempt = (selectedIndex: number) => {
    setSelectedAttemptIndex(selectedIndex);
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
    [inputValue]
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
              selectedValue={selectedAttemptIndex}
              options={attemptListboxOptions}
              onSelect={onSelectAttempt}
              isDisabled={job.attempts.length === 1}
            />
          </div>
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
          <DownloadLogsButton logLines={logLines} fileName={`job-${jobId}-attempt-${selectedAttemptIndex + 1}`} />
        </FlexContainer>
      </Box>
      <Box pl="xl" pr="md">
        <FlexContainer>
          {jobAttempt.attempt.endedAt && (
            <>
              <Text as="span" color="grey" size="sm">
                <FormattedTimeParts value={jobAttempt.attempt.createdAt * 1000} hour="numeric" minute="2-digit">
                  {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
                </FormattedTimeParts>
                <FormattedDate
                  value={jobAttempt.attempt.createdAt * 1000}
                  month="2-digit"
                  day="2-digit"
                  year="numeric"
                />
              </Text>
              <Text as="span" color="grey" size="sm">
                |
              </Text>
            </>
          )}
          <Text as="span" color="grey" size="sm">
            {formatBytes(jobAttempt.attempt.totalStats?.bytesEmitted)}
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countRecordsExtracted"
              values={{ count: jobAttempt.attempt.totalStats?.recordsEmitted || 0 }}
            />
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countRecordsLoaded"
              values={{ count: jobAttempt.attempt.totalStats?.recordsCommitted || 0 }}
            />
          </Text>
        </FlexContainer>
      </Box>
      <JobLogsModalFailureMessage failureSummary={jobAttempt.attempt.failureSummary} />
      <VirtualLogs
        selectedAttempt={selectedAttemptIndex}
        logLines={logLines}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
      />
    </FlexContainer>
  );
};
