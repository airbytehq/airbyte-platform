import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useDebounce } from "react-use";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { MultiListBox } from "components/ui/ListBox/MultiListBox";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { LogSearchInput } from "area/connection/components/JobHistoryItem/LogSearchInput";
import { LOG_LEVELS, LOG_SOURCE_REGEX_MAP, useCleanLogs } from "area/connection/components/JobHistoryItem/useCleanLogs";
import { VirtualLogs } from "area/connection/components/JobHistoryItem/VirtualLogs";
import { attemptHasStructuredLogs, AttemptInfoReadWithLogs } from "core/api";
import { LogLevel, LogSource } from "core/api/types/AirbyteClient";

import { JobLogsModalFailureMessage } from "./JobLogsModalFailureMessage";

interface AttemptLogsProps {
  attempt: AttemptInfoReadWithLogs;
}

export const AttemptLogs: React.FC<AttemptLogsProps> = ({ attempt }) => {
  const searchInputRef = useRef<HTMLInputElement>(null);

  const [inputValue, setInputValue] = useState("");
  const [highlightedMatchIndex, setHighlightedMatchIndex] = useState<number | undefined>(undefined);
  const [matchingLines, setMatchingLines] = useState<number[]>([]);
  const highlightedMatchingLineNumber = highlightedMatchIndex !== undefined ? highlightedMatchIndex + 1 : undefined;

  const showStructuredLogs = attempt && attemptHasStructuredLogs(attempt);

  const { logLines, sources, levels } = useCleanLogs(attempt);
  const [selectedLogLevels, setSelectedLogLevels] = useState<LogLevel[]>(LOG_LEVELS);
  const [selectedLogSources, setSelectedLogSources] = useState<LogSource[]>(LOG_SOURCE_REGEX_MAP.map(({ key }) => key));
  const firstMatchIndex = 0;
  const lastMatchIndex = matchingLines.length - 1;
  const [debouncedSearchTerm, setDebouncedSearchTerm] = useState("");
  const scrollTo = useMemo(
    () => (matchingLines && highlightedMatchIndex !== undefined ? matchingLines[highlightedMatchIndex] : undefined),
    [matchingLines, highlightedMatchIndex]
  );
  const { formatMessage } = useIntl();

  const logLevelOptions = useMemo<Array<{ label: string; value: LogLevel }>>(
    () =>
      LOG_LEVELS.map((level) => {
        return { label: formatMessage({ id: `jobHistory.logs.logLevel.${level}` }), value: level };
      }),
    [formatMessage]
  );

  const logSourceOptions = useMemo<Array<{ label: string; value: LogSource }>>(
    () =>
      LOG_SOURCE_REGEX_MAP.map(({ key }) => {
        return { label: formatMessage({ id: `jobHistory.logs.logSource.${key}` }), value: key };
      }),
    [formatMessage]
  );

  const onSelectLogSource = useCallback(
    (source: LogSource) => {
      if (!selectedLogSources) {
        setSelectedLogSources(sources.filter((s) => s !== source));
      } else {
        setSelectedLogSources(
          selectedLogSources.includes(source)
            ? selectedLogSources.filter((s) => s !== source)
            : [...selectedLogSources, source]
        );
      }
    },
    [sources, selectedLogSources]
  );

  const filteredLogLines = useMemo(() => {
    return logLines.filter((line) => {
      if (line.source && !selectedLogSources?.includes(line.source)) {
        return false;
      }
      if (line.level && !selectedLogLevels?.includes(line.level)) {
        return false;
      }
      return true;
    });
  }, [logLines, selectedLogSources, selectedLogLevels]);

  // Debounces changes to the search input so we don't recompute the matching lines on every keystroke
  useDebounce(
    () => {
      setDebouncedSearchTerm(inputValue);
      setHighlightedMatchIndex(undefined);
      const searchTermLowerCase = inputValue.toLowerCase();
      if (inputValue.length > 0) {
        const matchingLines: number[] = [];
        filteredLogLines.forEach((line, index) => {
          return line.original.toLocaleLowerCase().includes(searchTermLowerCase) && matchingLines.push(index);
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
    <>
      <JobLogsModalFailureMessage failureSummary={attempt.attempt.failureSummary} />
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
          {showStructuredLogs && (
            <>
              <FlexItem>
                <MultiListBox
                  selectedValues={selectedLogSources ?? sources}
                  options={logSourceOptions}
                  onSelectValues={(newSources) => setSelectedLogSources(newSources ?? sources)}
                  label="Log sources"
                />
              </FlexItem>
              <FlexItem>
                <MultiListBox
                  selectedValues={selectedLogLevels ?? levels}
                  options={logLevelOptions}
                  onSelectValues={(newLevels) => setSelectedLogLevels(newLevels ?? levels)}
                  label="Log levels"
                />
              </FlexItem>
            </>
          )}
        </FlexContainer>
      </Box>

      {sources.length > 0 && (
        <Box px="md">
          <FlexContainer gap="lg">
            {logSourceOptions.map((option) => (
              <label key={option.value}>
                <FlexContainer key={option.value} alignItems="center" as="span" display="inline-flex" gap="sm">
                  <Switch
                    size="xs"
                    checked={selectedLogSources?.includes(option.value) ?? true}
                    onChange={() => onSelectLogSource(option.value)}
                  />
                  <Text>{option.label}</Text>
                </FlexContainer>
              </label>
            ))}
          </FlexContainer>
        </Box>
      )}

      {logLines.length === 0 && (
        <Box p="xl">
          <FlexContainer justifyContent="center">
            <Text>
              <FormattedMessage id="jobHistory.logs.noLogsFound" />
            </Text>
          </FlexContainer>
        </Box>
      )}

      <VirtualLogs
        attemptId={attempt.attempt.id}
        logLines={filteredLogLines}
        searchTerm={debouncedSearchTerm}
        scrollTo={scrollTo}
        hasFailure={!!attempt.attempt.failureSummary}
        showStructuredLogs={showStructuredLogs}
      />
    </>
  );
};
