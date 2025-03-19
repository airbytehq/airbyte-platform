import Anser from "anser";
import classNames from "classnames";
import React, { HTMLAttributes, useEffect, useRef } from "react";
import Highlighter from "react-highlight-words";
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
  attemptId: number;
  hasFailure: boolean;
  showStructuredLogs: boolean;
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
  showStructuredLogs: boolean;
}

const LogLine: React.FC<HTMLAttributes<HTMLDivElement>> = (props) => (
  <div {...props} className={styles.virtualLogs__line} />
);
LogLine.displayName = "LogLine";

const VirtualLogsUnmemoized: React.FC<VirtualLogsProps> = ({
  logLines,
  searchTerm,
  scrollTo,
  attemptId,
  hasFailure,
  showStructuredLogs,
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
          key={attemptId}
          style={{ width: "100%", height: "100%" }}
          data={logLines}
          itemContent={Row}
          context={{ searchTerm, highlightedRowIndex, showStructuredLogs }}
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

interface SearchMatchInLogLine {
  precedingNewlines: number; // For multi-line logs, we need to know how far to offset the highlighter from the top
  characterOffsetLeft: number;
}
/**
 * Given a log line with newlines, we need to find both the top and left offset to correctly highlight it.
 *
 * For example, when searching for the character "e" in this log line:
 * "some\n text"
 *
 * We split the lines into chunks and find matches at the following indices:
 * ["some", "text"]
 *      ^     ^
 * Where the first match is: { precedingNewlines: 0, characterOffsetLeft: 3 }
 * And the second match is:  { precedingNewlines: 1, characterOffsetLeft: 1 }
 */
export const getSearchMatchesInLine = (text: string, searchTerm?: string) => {
  const matchIndices: SearchMatchInLogLine[] = [];
  if (searchTerm) {
    const escapedSearchTerm = escapeRegex(searchTerm);
    const regex = new RegExp(escapedSearchTerm, "gi");
    let match;
    while ((match = regex.exec(text)) !== null) {
      const chunksBetweenNewlines = text.slice(0, match.index).split("\n");
      const targetIndex = match.index; // The search match index in the original string, including linebreak characters
      let counter = 0;
      let leftOffsetFromStartOfChunk = 0;
      for (let i = 0; i < chunksBetweenNewlines.length; i++) {
        const chunk = chunksBetweenNewlines[i];
        if (counter + chunk.length >= targetIndex) {
          leftOffsetFromStartOfChunk = targetIndex - counter;
          break;
        }
        counter += chunk.length + 1; // +1 to account for the '\n' that gets stripped out from .split()
      }

      matchIndices.push({
        precedingNewlines: text.slice(0, match.index).match(/\n/g)?.length ?? 0,
        characterOffsetLeft: leftOffsetFromStartOfChunk,
      });
    }
  }
  return matchIndices;
};

const HighlightedSearchMatches = React.memo(({ text, searchTerm }: { text: string; searchTerm?: string }) => {
  const searchMatchesInLine = getSearchMatchesInLine(expandTabs(text), searchTerm);

  return (
    <>
      {searchMatchesInLine.map((match, index) => (
        <span
          key={index}
          className={styles.virtualLogs__searchMatch}
          style={{
            top: `${match.precedingNewlines * 1.5}em`,
            left: `${match.characterOffsetLeft}ch`,
          }}
        >
          {searchTerm}
        </span>
      ))}
    </>
  );
});
HighlightedSearchMatches.displayName = "HighlightedSearchMatches";

export const Row: ItemContent<CleanedLogLines[number], RowContext> = (index, item, context) => {
  const rowIsHighlighted = context.highlightedRowIndex === index;
  const html = Anser.ansiToHtml(expandTabs(item.original), { use_classes: true });

  return (
    <div
      className={classNames(styles.virtualLogs__lineInner, {
        [styles["virtualLogs__lineInner--highlighted"]]: rowIsHighlighted,
      })}
    >
      <div className={styles.virtualLogs__lineNumber}>{item.lineNumber}</div>
      <div className={styles.virtualLogs__lineLogContent}>
        {context.showStructuredLogs && (
          <>
            <Highlighter
              autoEscape
              className={styles.virtualLogs__timestamp}
              searchWords={[context.searchTerm || ""]}
              textToHighlight={item.timestamp || ""}
            />
            <SpaceForCopy />
            <Highlighter
              className={classNames(styles.virtualLogs__logSource, {
                [styles["virtualLogs__logSource--replicationOrchestrator"]]: item.source === "replication-orchestrator",
                [styles["virtualLogs__logSource--source"]]: item.source === "source",
                [styles["virtualLogs__logSource--destination"]]: item.source === "destination",
                [styles["virtualLogs__logSource--platform"]]: item.source === "platform",
              })}
              autoEscape
              searchWords={[context.searchTerm || ""]}
              textToHighlight={item.source || ""}
            />
            <SpaceForCopy />
            <Highlighter
              className={classNames(styles.virtualLogs__level, {
                [styles["virtualLogs__level--info"]]: item.level === "info",
                [styles["virtualLogs__level--warn"]]: item.level === "warn",
                [styles["virtualLogs__level--error"]]: item.level === "error",
                [styles["virtualLogs__level--debug"]]: item.level === "debug",
                [styles["virtualLogs__level--trace"]]: item.level === "trace",
              })}
              autoEscape
              searchWords={[context.searchTerm || ""]}
              textToHighlight={item.level?.toUpperCase() || ""}
            />
            <SpaceForCopy />
            <Highlighter autoEscape searchWords={[context.searchTerm || ""]} textToHighlight={item.text} />
          </>
        )}
        {!context.showStructuredLogs && (
          <>
            <DangerousHTML html={html} />
            <HighlightedSearchMatches text={item.text} searchTerm={context.searchTerm} />
          </>
        )}
      </div>
    </div>
  );
};

// JSX makes this hard. When copy and pasting, we want a space between the elements.
const SpaceForCopy = () => <> </>;

// The length of the logLines is a fine proxy to tell if they have changed, which can avoid re-renders
export const VirtualLogs = React.memo(
  VirtualLogsUnmemoized,
  (prevProps, nextProps) =>
    prevProps.logLines.length === nextProps.logLines.length &&
    prevProps.searchTerm === nextProps.searchTerm &&
    prevProps.scrollTo === nextProps.scrollTo &&
    prevProps.attemptId === nextProps.attemptId &&
    prevProps.showStructuredLogs === nextProps.showStructuredLogs
);
