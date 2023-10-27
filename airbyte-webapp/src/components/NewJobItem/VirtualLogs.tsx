import Anser from "anser";
import classNames from "classnames";
import React, { HTMLAttributes, useEffect, useRef } from "react";
import { FormattedMessage } from "react-intl";
import { Virtuoso, ItemContent, VirtuosoHandle } from "react-virtuoso";
import sanitize from "sanitize-html";

import { Text } from "components/ui/Text";

import { CleanedLogLines } from "./useCleanLogs";
import styles from "./VirtualLogs.module.scss";

interface VirtualLogsProps {
  logLines: CleanedLogLines;
  searchTerm?: string;
  scrollTo?: number;
  selectedAttempt?: number;
  hasFailure: boolean;
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

interface RowContext {
  searchTerm?: string;
  highlightedRowIndex?: number;
}

const LogLine: React.FC<HTMLAttributes<HTMLDivElement>> = (props) => (
  <div {...props} className={styles.virtualLogs__line} />
);
LogLine.displayName = "LogLine";

const VirtualLogsUnmemoized: React.FC<VirtualLogsProps> = ({
  logLines,
  searchTerm,
  scrollTo,
  selectedAttempt,
  hasFailure,
}) => {
  const listRef = useRef<VirtuosoHandle | null>(null);
  const highlightedRowIndex = scrollTo;

  useEffect(() => {
    if (scrollTo !== undefined) {
      listRef.current?.scrollIntoView({ index: scrollTo, align: "center" });
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
        <Virtuoso<CleanedLogLines[number], RowContext>
          ref={listRef}
          initialTopMostItemIndex={{ index: "LAST" }}
          followOutput={
            // smooth scroll unless there's an error, the appearance of the error message decreases
            // the logs viewport area which invalides the target scroll position during a smooth
            // scroll, which results in not positioning at the bottom
            (isAtBottom) => isAtBottom && (hasFailure ? true : "smooth")
          }
          key={selectedAttempt}
          style={{ width: "100%", height: "100%" }}
          data={logLines}
          itemContent={Row}
          context={{ searchTerm, highlightedRowIndex }}
          atBottomThreshold={50 /* covers edge case(s) where Virtuoso doesn't scroll all the way to the bottom */}
          increaseViewportBy={150}
          components={{
            Item: LogLine,
          }}
        />
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

const Row: ItemContent<CleanedLogLines[number], RowContext> = (index, item, context) => {
  const rowIsHighlighted = context.highlightedRowIndex === index;
  const html = Anser.ansiToHtml(expandTabs(item.original), { use_classes: true });
  const matchIndices = getMatchIndices(expandTabs(item.text), context.searchTerm);

  return (
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
              style={{ left: `${matchIndex}ch`, width: `${context.searchTerm?.length}ch` }}
            />
          ))}
        <DangerousHTML html={html} />
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
