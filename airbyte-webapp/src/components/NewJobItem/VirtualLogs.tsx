import Anser from "anser";
import classNames from "classnames";
import React, { useEffect, useRef } from "react";
import { FormattedMessage } from "react-intl";
import AutoSizer from "react-virtualized-auto-sizer";
import { FixedSizeList, ListChildComponentProps } from "react-window";
import sanitize from "sanitize-html";

import { Text } from "components/ui/Text";

import { CleanedLogLines } from "./useCleanLogs";
import styles from "./VirtualLogs.module.scss";

interface VirtualLogsProps {
  logLines: CleanedLogLines;
  searchTerm?: string;
  scrollTo?: number;
  selectedAttempt?: number;
}

function escapeRegex(string: string) {
  return string.replace(/[/\-\\^$*+?.()|[\]{}]/g, "\\$&");
}

function expandTabs(string: string) {
  return string.replace(/\t/g, "        ");
}

/**
 * Sanitizes a given string, allowing only specifically whitelisted tags and attributes
 */
export const sanitizeHtml = (logLine: string) => {
  return sanitize(logLine, {
    allowedTags: ["span"],
    allowedAttributes: {
      span: ["class"],
    },
    disallowedTagsMode: "escape",
    enforceHtmlBoundary: false,
  });
};

interface RowData {
  logLines: CleanedLogLines;
  searchTerm?: string;
  highlightedRowIndex?: number;
}

const VirtualLogsUnmemoized: React.FC<VirtualLogsProps> = ({ logLines, searchTerm, scrollTo, selectedAttempt }) => {
  const listRef = useRef<FixedSizeList>(null);
  const highlightedRowIndex = scrollTo;

  useEffect(() => {
    if (scrollTo !== undefined) {
      listRef.current?.scrollToItem(scrollTo);
    }
  }, [scrollTo]);

  return (
    <div className={styles.virtualLogs}>
      {!logLines && (
        <Text>
          <FormattedMessage id="jobHistory.logs.noLogs" />
        </Text>
      )}
      {logLines && (
        <AutoSizer>
          {({ height, width }) =>
            height && width ? (
              <FixedSizeList<RowData>
                key={selectedAttempt}
                height={height}
                itemCount={logLines.length}
                itemSize={20}
                itemData={{ logLines, searchTerm, highlightedRowIndex }}
                width={width}
                ref={listRef}
                overscanCount={10}
              >
                {Row}
              </FixedSizeList>
            ) : null
          }
        </AutoSizer>
      )}
    </div>
  );
};

const DangerousHTML = ({ html }: { html: string }) => {
  // eslint-disable-next-line react/no-danger
  return <div dangerouslySetInnerHTML={{ __html: sanitizeHtml(html) }} />;
};

export const getMatchIndices = (text: string, searchTerm?: string) => {
  const matchIndices: number[] = [];
  if (searchTerm) {
    const escapedSearchTerm = escapeRegex(searchTerm);
    const regex = new RegExp(escapedSearchTerm, "gi");
    let match;
    while ((match = regex.exec(text)) !== null) {
      matchIndices.push(match.index);
    }
  }
  return matchIndices;
};

const Row: React.FC<ListChildComponentProps<RowData>> = ({ index, style, data }) => {
  const rowIsHighlighted = data.highlightedRowIndex === index;
  const html = Anser.ansiToHtml(expandTabs(data.logLines[index].original), { use_classes: true });
  const matchIndices = getMatchIndices(expandTabs(data.logLines[index].text), data.searchTerm);

  return (
    <div style={style} className={styles.virtualLogs__line}>
      <div
        className={classNames(styles.virtualLogs__lineInner, {
          [styles["virtualLogs__lineInner--highlighted"]]: rowIsHighlighted,
        })}
      >
        <div className={styles.virtualLogs__lineNumber}>{index + 1}</div>
        <div className={styles.virtualLogs__lineLogContent}>
          {matchIndices.length > 0 &&
            matchIndices.map((matchIndex) => (
              <div
                className={styles.virtualLogs__searchMatch}
                key={matchIndex}
                style={{ left: `${matchIndex}ch`, width: `${data.searchTerm?.length}ch` }}
              />
            ))}
          <DangerousHTML html={html} />
        </div>
      </div>
    </div>
  );
};

// The length of the logLines is a fine proxy to tell if they have changed, which can avoid re-renders
export const VirtualLogs = React.memo(
  VirtualLogsUnmemoized,
  (prevProps, nextProps) =>
    prevProps.logLines.length === nextProps.logLines.length &&
    prevProps.searchTerm === nextProps.searchTerm &&
    prevProps.scrollTo === nextProps.scrollTo &&
    prevProps.selectedAttempt === nextProps.selectedAttempt
);
