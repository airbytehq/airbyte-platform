import Anser from "anser";
import { useMemo } from "react";

import { AttemptInfoRead } from "core/api/types/AirbyteClient";

export type CleanedLogLines = Array<{
  original: string;
  text: string;
  domain?: LogDomains;
}>;

export enum LogDomains {
  ReplicationOrchestrator = "replication-orchestrator",
  Source = "source",
  Destination = "destination",
  None = "none",
}

// Not currently used, but could be useful for grouping/filtering/coloring log lines by domain
const KNOWN_LOG_DOMAINS = [
  { key: LogDomains.ReplicationOrchestrator, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} replication-orchestrator/ },
  { key: LogDomains.Source, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} source/ },
  { key: LogDomains.Destination, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} destination/ },
  // If the log starts with a timestamp but then doesn't match any of the above, it's considered not matching any domain
  // which is helpful to start a new block of log lines that does not have any color-coding
  { key: LogDomains.None, regex: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} / },
];

/**
 * useCleanLogs iterates through each log line of each attempt and transforms it to be more easily consumed by the UI.
 */
export const useCleanLogs = (attempt: AttemptInfoRead): CleanedLogLines => {
  const cleanedLogs = useMemo(() => {
    // Some logs are multi-line, so we want to associate those lines (which might not have the correct prefix) with the last domain that was detected
    let lastDomain: LogDomains | undefined;
    const result = attempt.logs.logLines.map((line) => {
      const text = Anser.ansiToText(line);
      const domain = KNOWN_LOG_DOMAINS.find((domain) => domain.regex.test(text))?.key;
      if (domain) {
        lastDomain = domain;
      }
      return {
        original: line,
        text,
        domain: domain ?? lastDomain,
      };
    });
    return result;
  }, [attempt]);

  return cleanedLogs;
};
