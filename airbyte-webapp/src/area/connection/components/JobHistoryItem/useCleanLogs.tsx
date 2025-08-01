import Anser from "anser";
import dayjs from "dayjs";
import { useMemo } from "react";

import { attemptHasFormattedLogs, attemptHasStructuredLogs } from "core/api";
import { AttemptInfoRead, LogEvent, LogLevel, LogSource } from "core/api/types/AirbyteClient";

export interface CleanedLogs {
  sources: LogSource[];
  levels: LogLevel[];
  logLines: CleanedLogLines;
}

export type CleanedLogLines = Array<{
  lineNumber: number;
  original: string;
  text: string;
  level?: LogLevel;
  source?: LogSource;
  timestamp?: string;
}>;

// This can be removed once we switch over entirely to structured logs
// https://github.com/airbytehq/airbyte-internal-issues/issues/10658
export const LOG_SOURCE_REGEX_MAP = [
  {
    key: LogSource["replication-orchestrator"],
    regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} replication-orchestrator/,
  },
  { key: LogSource.source, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} source/ },
  { key: LogSource.destination, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} destination/ },
  { key: LogSource.platform, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} platform/ },
];

export const LOG_LEVELS: LogLevel[] = ["info", "warn", "error", "debug", "trace"];

/**
 * useCleanLogs iterates through each log line of each attempt and transforms it to be more easily consumed by the UI.
 */
export const useCleanLogs = (attempt: AttemptInfoRead): CleanedLogs => {
  return useMemo(() => {
    const levels = new Set<LogLevel>();
    const sources = new Set<LogSource>();

    if (attemptHasFormattedLogs(attempt)) {
      const logLines = attempt.logs.logLines.map((line, index) => {
        const text = Anser.ansiToText(line);
        const source = LOG_SOURCE_REGEX_MAP.find((source) => source.regex.test(text))?.key;
        if (source) {
          sources.add(source);
        }
        return {
          lineNumber: index + 1,
          original: line,
          text,
          source,
        };
      });
      return {
        sources: [...sources],
        levels: [...levels],
        logLines,
      };
    }

    if (attemptHasStructuredLogs(attempt)) {
      const logLines = attempt.logs.events.map((event, index) => {
        levels.add(event.level);
        const messageWithoutLogLevel = event.message.replace(beginsWithLogLevel, "");
        return {
          lineNumber: index + 1,
          original: formatLogEvent({ ...event, message: messageWithoutLogLevel }),
          text: messageWithoutLogLevel,
          source: event.logSource,
          level: event.level,
          timestamp: formatLogEventTimestamp(event.timestamp),
        };
      });
      return {
        sources: [...sources],
        levels: [...levels],
        logLines,
      };
    }

    throw new Error("Log format unsupported. Only formatted or structured logs are supported.");
  }, [attempt]);
};

// Filters out the log level from the beginning of the log message, because connector logs hard-code this as part of
// the log message. Structured logs from the platform (using logback) do not have this issue.
const beginsWithLogLevel = new RegExp(`^(${LOG_LEVELS.map((level) => level.toUpperCase()).join("|")})\\s*`);

export function formatLogEvent(event: LogEvent): string {
  return `${formatLogEventTimestamp(event.timestamp)} ${event.level} ${event.message}`;
}

export function formatLogEventTimestamp(unixTimestamp: number): string {
  // Intentionally not internationalized to match expectations for log timestamps
  return dayjs(unixTimestamp).format("YYYY-MM-DD HH:mm:ss");
}
