/**
 * Recursively compares two configuration objects and returns an array of
 * dot-notation field paths that have changed.
 *
 * @param oldConfig - The previous configuration
 * @param newConfig - The current configuration
 * @param prefix - The base path prefix (e.g., "connectionConfiguration")
 * @returns Array of changed field paths like ["connectionConfiguration.password", "connectionConfiguration.host"]
 *
 * @example
 * const oldConfig = { host: "localhost", password: "old" };
 * const newConfig = { host: "newhost", password: "old" };
 * const changed = findChangedFieldPaths(oldConfig, newConfig);
 * // Returns: ["connectionConfiguration.host"]
 *
 * @example
 * // Works with nested objects
 * const oldConfig = { tunnel: { host: "old", port: 22 } };
 * const newConfig = { tunnel: { host: "new", port: 22 } };
 * const changed = findChangedFieldPaths(oldConfig, newConfig);
 * // Returns: ["connectionConfiguration.tunnel.host"]
 */
export const findChangedFieldPaths = (
  oldConfig: Record<string, unknown>,
  newConfig: Record<string, unknown>,
  prefix: string = "connectionConfiguration"
): string[] => {
  const changedPaths: string[] = [];

  const compareValues = (oldVal: unknown, newVal: unknown, path: string): void => {
    // For primitives, arrays, null/undefined - compare directly
    if (
      typeof oldVal !== "object" ||
      typeof newVal !== "object" ||
      oldVal === null ||
      newVal === null ||
      Array.isArray(oldVal) ||
      Array.isArray(newVal)
    ) {
      if (JSON.stringify(oldVal) !== JSON.stringify(newVal)) {
        changedPaths.push(path);
      }
      return;
    }

    // For objects - recursively compare all keys
    const oldObj = oldVal as Record<string, unknown>;
    const newObj = newVal as Record<string, unknown>;
    const allKeys = new Set([...Object.keys(oldObj), ...Object.keys(newObj)]);

    allKeys.forEach((key) => {
      const fieldPath = `${path}.${key}`;
      compareValues(oldObj[key], newObj[key], fieldPath);
    });
  };

  compareValues(oldConfig, newConfig, prefix);
  return changedPaths;
};
