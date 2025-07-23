/**
 * Determines if a cron expression executes more frequently than once per hour
 * using the backend validation response.
 *
 * @param nextExecutions - Array of next execution timestamps from backend
 * @returns true if the cron runs more than once per hour, false otherwise
 */
export function isMoreFrequentThanHourlyFromExecutions(nextExecutions: number[]): boolean {
  if (!nextExecutions || nextExecutions.length < 2) {
    // If we don't have at least 2 executions, we can't determine frequency
    return false;
  }

  // Check the interval between the first two executions
  const firstExecution = nextExecutions[0];
  const secondExecution = nextExecutions[1];
  let intervalMs = secondExecution - firstExecution;

  // Handle potential seconds-to-milliseconds conversion
  // If the timestamps appear to be in seconds format (much smaller than typical millisecond timestamps)
  // Typical millisecond timestamp is around 1.7 * 10^12, seconds would be around 1.7 * 10^9
  if (firstExecution < 1e10) {
    // Likely in seconds, convert to milliseconds
    intervalMs = intervalMs * 1000;
  }

  // One hour in milliseconds
  const oneHourMs = 60 * 60 * 1000;

  // Additional safety check: if interval is negative or zero, return false
  if (intervalMs <= 0) {
    return false;
  }

  // If the interval is less than one hour, it's more frequent than hourly
  return intervalMs < oneHourMs;
}
