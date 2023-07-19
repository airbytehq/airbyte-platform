import { useEffect, useMemo, useRef, useState } from "react";
import { FormattedDate, FormattedMessage, FormattedTimeParts, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useGetDebugInfoJob } from "core/api";
import { FailureType } from "core/request/AirbyteClient";
import { formatBytes } from "core/utils/numberHelper";

import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModalContent.module.scss";
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
  const debugInfo = useGetDebugInfoJob(jobId, typeof jobId === "number", true);
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;
  const [selectedAttemptIndex, setSelectedAttemptIndex] = useState<number>(debugInfo.attempts.length - 1);
  const cleanedLogs = useCleanLogs(debugInfo);
  const logLines = useMemo(() => cleanedLogs.attempts[selectedAttemptIndex], [cleanedLogs, selectedAttemptIndex]);
  const firstMatchIndex = 0;
  const lastMatchIndex = matchingLines.length - 1;
  const [debouncedSearchTerm, setDebouncedSearchTerm] = useState("");
  const scrollTo = useMemo(
    () => (matchingLines && highlightedMatchIndex !== undefined ? matchingLines[highlightedMatchIndex] : undefined),
    [matchingLines, highlightedMatchIndex]
  );
  const { formatMessage } = useIntl();

  const attemptListboxOptions = useMemo(() => {
    return debugInfo.attempts.map((_, index) => ({
      label: formatMessage(
        { id: "jobHistory.logs.attemptLabel" },
        { attemptNumber: index + 1, totalAttempts: debugInfo.attempts.length }
      ),
      value: index,
      icon: <AttemptStatusIcon attempt={debugInfo.attempts[index]} />,
    }));
  }, [debugInfo, formatMessage]);

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

  const showFailureReason = useMemo(
    () =>
      debugInfo.attempts[selectedAttemptIndex].attempt?.failureSummary?.failures[0] &&
      !debugInfo.attempts[selectedAttemptIndex].attempt.failureSummary?.failures.some(
        ({ failureType }) => failureType === FailureType.manual_cancellation
      ),
    [debugInfo, selectedAttemptIndex]
  );

  const failureReason = useMemo(
    () => debugInfo.attempts[selectedAttemptIndex].attempt?.failureSummary?.failures[0]?.internalMessage,
    [debugInfo, selectedAttemptIndex]
  );

  const selectedAttempt = useMemo(
    () => debugInfo.attempts[selectedAttemptIndex],
    [debugInfo.attempts, selectedAttemptIndex]
  );

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
              isDisabled={debugInfo.attempts.length === 1}
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
          {selectedAttempt.attempt.endedAt && (
            <>
              <Text as="span" color="grey" size="sm">
                <FormattedTimeParts value={selectedAttempt.attempt.createdAt * 1000} hour="numeric" minute="2-digit">
                  {(parts) => <span>{`${parts[0].value}:${parts[2].value}${parts[4].value} `}</span>}
                </FormattedTimeParts>
                <FormattedDate
                  value={selectedAttempt.attempt.createdAt * 1000}
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
            {formatBytes(selectedAttempt.attempt.totalStats?.bytesEmitted)}
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countEmittedRecords"
              values={{ count: selectedAttempt.attempt.totalStats?.recordsEmitted || 0 }}
            />
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countCommittedRecords"
              values={{ count: selectedAttempt.attempt.totalStats?.recordsCommitted || 0 }}
            />
          </Text>
        </FlexContainer>
      </Box>

      {showFailureReason && (
        <Box pl="xl" pr="md">
          <Text color="grey" size="sm">
            {failureReason ? (
              <FormattedMessage id="jobHistory.logs.failureReason" values={{ reason: failureReason }} />
            ) : (
              <FormattedMessage id="errorView.unknown" />
            )}
          </Text>
        </Box>
      )}
      <VirtualLogs
        selectedAttempt={selectedAttemptIndex}
        logLines={logLines}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
      />
    </FlexContainer>
  );
};
