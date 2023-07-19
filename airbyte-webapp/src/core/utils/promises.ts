/**
 * Returns a promise that rejects after `delay` milliseconds with the given reason.
 */
export function rejectAfter(delay: number, reason: string) {
  return new Promise((_, reject) => {
    window.setTimeout(() => reject(reason), delay);
  });
}

/**
 * Asserts that the given settled promise has been fulfilled. Useful for filtering out rejected promises:
 *
 * const results = await Promise.allSettled(promises);
 * const fulfilled = results.filter(isFulfilled); // fulfilled is typed correctly as PromiseFulfilledResult
 */
export const isFulfilled = <T>(p: PromiseSettledResult<T>): p is PromiseFulfilledResult<T> => p.status === "fulfilled";
