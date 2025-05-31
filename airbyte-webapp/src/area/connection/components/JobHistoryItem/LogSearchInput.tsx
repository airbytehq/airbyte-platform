import React from "react";
import { useIntl } from "react-intl";
import { useMeasure } from "react-use";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import styles from "./LogSearchInput.module.scss";

interface LogSearchInputProps {
  inputValue: string;
  onSearchTermChange: (searchTerm: string) => void;
  onSearchInputKeydown: (e: React.KeyboardEvent<HTMLInputElement>) => void;
  highlightedMatchIndex: number | undefined;
  matches: number[];
  scrollToPreviousMatch: () => void;
  scrollToNextMatch: () => void;
  highlightedMatchDisplay: number | undefined;
}

export const LogSearchInput = React.forwardRef<HTMLInputElement, LogSearchInputProps>(
  (
    {
      inputValue,
      onSearchInputKeydown,
      onSearchTermChange,
      highlightedMatchDisplay,
      highlightedMatchIndex,
      matches,
      scrollToNextMatch,
      scrollToPreviousMatch,
    },
    ref
  ) => {
    const [lineCountRef, { width }] = useMeasure<HTMLDivElement>();
    const { formatMessage } = useIntl();
    const hasHighlightedMatch = highlightedMatchIndex !== undefined;

    return (
      <div className={styles.logSearchInput__inputWrapper}>
        <SearchInput
          ref={ref}
          value={inputValue}
          onChange={(e) => onSearchTermChange(e.target.value)}
          onKeyDown={onSearchInputKeydown}
          placeholder={formatMessage({ id: "jobHistory.logs.searchPlaceholder" })}
          style={{ paddingRight: (hasHighlightedMatch ? width : 0) + 15 }}
          light
        />
        {hasHighlightedMatch && (
          <div className={styles.logSearchInput__searchControls} ref={lineCountRef}>
            <Text align="center" size="xs" color={inputValue.length === 0 ? "grey" : "darkBlue"}>
              {`${highlightedMatchDisplay} / ${matches.length}`}
            </Text>
            <FlexContainer gap="none">
              <button
                className={styles.logSearchInput__button}
                aria-label={formatMessage({ id: "jobHistory.logs.previousMatchLabel" })}
                disabled={matches.length <= 1}
                onClick={scrollToPreviousMatch}
                type="button"
              >
                <Icon type="chevronLeft" color={matches.length > 1 ? "affordance" : "disabled"} />
              </button>
              <button
                className={styles.logSearchInput__button}
                aria-label={formatMessage({ id: "jobHistory.logs.nextMatchLabel" })}
                disabled={matches.length <= 1}
                onClick={scrollToNextMatch}
                type="button"
              >
                <Icon type="chevronRight" color={matches.length > 1 ? "affordance" : "disabled"} />
              </button>
            </FlexContainer>
          </div>
        )}
      </div>
    );
  }
);

LogSearchInput.displayName = "LogSearchInput";
