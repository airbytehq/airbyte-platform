import { useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { JobWithAttempts } from "components/JobItem/types";
import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { Text } from "components/ui/Text";

import { useGetDebugInfoJob } from "core/api";
import { formatBytes } from "utils/numberHelper";

import { DownloadLogsButton } from "./DownloadLogsButton";
import styles from "./JobLogsModalContent.module.scss";
import { AttemptStatusIcon } from "./JobStatusIcon";
import { LogSearchInput } from "./LogSearchInput";
import { useCleanLogs } from "./useCleanLogs";
import { VirtualLogs } from "./VirtualLogs";

interface JobLogsModalContentProps {
  jobId: number;
  job: JobWithAttempts;
}

export const JobLogsModalContent: React.FC<JobLogsModalContentProps> = ({ jobId, job }) => {
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [inputValue, setInputValue] = useState("");
  const debugInfo = useGetDebugInfoJob(jobId, typeof jobId === "number", true);
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;
  const [selectedAttemptIndex, setSelectedAttemptIndex] = useState<number>(debugInfo.attempts.length - 1);
  const cleanedLogs = useCleanLogs(debugInfo);
  const attempt = useMemo(() => cleanedLogs.attempts[selectedAttemptIndex], [cleanedLogs, selectedAttemptIndex]);
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

  useDebounce(
    () => {
      setDebouncedSearchTerm(inputValue);
      setHighlightedMatchIndex(undefined);
      const searchTermLowerCase = inputValue.toLowerCase();
      if (inputValue.length > 0) {
        const matchingLines: number[] = [];
        attempt.forEach(
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
    100,
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
          <DownloadLogsButton logLines={attempt} fileName={`job-${jobId}`} />
        </FlexContainer>
      </Box>
      <Box pl="xl" pr="md">
        <FlexContainer>
          <Text as="span" color="grey" size="sm">
            {formatBytes(job.attempts[selectedAttemptIndex].totalStats?.bytesEmitted)}
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countEmittedRecords"
              values={{ count: job.attempts[selectedAttemptIndex].totalStats?.recordsEmitted || 0 }}
            />
          </Text>
          <Text as="span" color="grey" size="sm">
            |
          </Text>
          <Text as="span" color="grey" size="sm">
            <FormattedMessage
              id="sources.countCommittedRecords"
              values={{ count: job.attempts[selectedAttemptIndex].totalStats?.recordsCommitted || 0 }}
            />
          </Text>
        </FlexContainer>
      </Box>
      <VirtualLogs
        selectedAttempt={selectedAttemptIndex}
        logLines={attempt}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
      />
    </FlexContainer>
  );
};
