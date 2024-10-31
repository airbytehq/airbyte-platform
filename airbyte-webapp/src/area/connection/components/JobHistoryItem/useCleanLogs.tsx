import Anser from "anser";
import { useMemo } from "react";

import { attemptHasFormattedLogs } from "core/api";
import { AttemptInfoRead } from "core/api/types/AirbyteClient";

export interface CleanedLogs {
  origins: JobLogOrigins[];
  logLines: CleanedLogLines;
}

export type CleanedLogLines = Array<{
  lineNumber: number;
  original: string;
  text: string;
  domain?: JobLogOrigins;
}>;

export enum JobLogOrigins {
  Destination = "destination",
  Platform = "platform",
  Other = "other",
  ReplicationOrchestrator = "replication-orchestrator",
  Source = "source",
}

export const KNOWN_LOG_ORIGINS = [
  {
    key: JobLogOrigins.ReplicationOrchestrator,
    regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} replication-orchestrator/,
  },
  { key: JobLogOrigins.Source, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} source/ },
  { key: JobLogOrigins.Destination, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} destination/ },
  { key: JobLogOrigins.Platform, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} platform/ },
  // If the log starts with a timestamp but then doesn't match any of the above, it's considered not matching any domain
  // which is helpful to start a new block of log lines that does not have any color-coding
  { key: JobLogOrigins.Other, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} / },
];

/**
 * useCleanLogs iterates through each log line of each attempt and transforms it to be more easily consumed by the UI.
 */
export const useCleanLogs = (attempt: AttemptInfoRead): CleanedLogs => {
  return useMemo(() => {
    const origins: JobLogOrigins[] = [];
    // Some logs are multi-line, so we want to associate those lines (which might not have the correct prefix) with the last domain that was detected
    let lastDomain: JobLogOrigins | undefined;

    if (attemptHasFormattedLogs(attempt)) {
      const logLines = attempt.logs.logLines.map((line, index) => {
        const text = Anser.ansiToText(line);
        const domain = KNOWN_LOG_ORIGINS.find((domain) => domain.regex.test(text))?.key;
        if (domain) {
          lastDomain = domain;
          if (!origins.includes(domain)) {
            origins.push(domain);
          }
        }
        return {
          lineNumber: index + 1,
          original: line,
          text,
          domain: domain ?? lastDomain,
        };
      });
      return {
        origins,
        logLines,
      };
    }

    // Structured logs are currently not supported in the UI:
    // https://github.com/airbytehq/airbyte-internal-issues/issues/10476
    return {
      origins,
      logLines: [],
    };
  }, [attempt]);
};
