import { useLocation } from "react-router-dom";

import { AttemptRead } from "core/api/types/AirbyteClient";

const PARSE_REGEXP = /^#(?<jobId>\w*)::(?<attemptId>\w*)$/;

// With the new job logs design, we will not allow linking to a specific attempt.
const NEW_PARSE_REGEXP = /^#(?<jobId>\w*)$/;

/**
 * Create and returns a link for a specific job and (optionally) attempt.
 * The returned string is the hash part of a URL.
 */
export const buildAttemptLink = (jobId: number | string, attemptId?: AttemptRead["id"]): string => {
  return `#${jobId}::${attemptId ?? ""}`;
};

/**
 * Parses a hash part of the URL into a jobId and attemptId.
 * This is the reverse function of {@link buildAttemptLink}.
 */
export const parseAttemptLink = (link: string): { jobId?: string; attemptId?: AttemptRead["id"] } => {
  const match = link.match(PARSE_REGEXP);
  const newMatch = link.match(NEW_PARSE_REGEXP);
  if (match) {
    return {
      jobId: match.groups?.jobId,
      attemptId: match.groups?.attemptId ? Number(match.groups.attemptId) : undefined,
    };
  }
  if (newMatch) {
    return {
      jobId: newMatch.groups?.jobId,
    };
  }
  return {};
};

/**
 * Returns the information about which attempt was linked to from the hash if available.
 */
export const useAttemptLink = () => {
  const { hash } = useLocation();
  return parseAttemptLink(hash);
};
