import React from "react";
import { useIntl } from "react-intl";
import { useMeasure } from "react-use";

import { Button } from "components/ui/Button";
import { Icon } from "components/ui/Icon";
import { Input } from "components/ui/Input";
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

    return (
      <>
        <div className={styles.logSearchInput__inputWrapper}>
          <Input
            ref={ref}
            value={inputValue}
            onChange={(e) => onSearchTermChange(e.target.value)}
            onKeyDown={onSearchInputKeydown}
            placeholder={formatMessage({ id: "jobHistory.logs.searchPlaceholder" })}
            style={{ paddingRight: width + 15 }}
            light
          />
          <div className={styles.logSearchInput__lineCount} ref={lineCountRef}>
            <Text align="center" size="xs" color={inputValue.length === 0 ? "grey" : "darkBlue"}>
              {highlightedMatchIndex !== undefined && `${highlightedMatchDisplay} / ${matches.length}`}
            </Text>
          </div>
        </div>
        <Button
          aria-label={formatMessage({ id: "jobHistory.logs.previousMatchLabel" })}
          disabled={matches.length === 0}
          onClick={scrollToPreviousMatch}
          type="button"
          variant="secondary"
          size="xs"
          icon={<Icon type="chevronLeft" />}
        />
        <Button
          aria-label={formatMessage({ id: "jobHistory.logs.nextMatchLabel" })}
          disabled={matches.length === 0}
          onClick={scrollToNextMatch}
          type="submit"
          variant="secondary"
          size="xs"
          icon={<Icon type="chevronRight" />}
        />
      </>
    );
  }
);

LogSearchInput.displayName = "LogSearchInput";
