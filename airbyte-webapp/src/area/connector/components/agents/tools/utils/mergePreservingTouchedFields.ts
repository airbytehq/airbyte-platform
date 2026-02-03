/**
 * Checks if any path in the touched fields set starts with the given field path.
 * This is used to detect if a field or any of its nested children have been touched.
 *
 * @param fieldPath - The field path to check (e.g., "connectionConfiguration.tunnel_method")
 * @param touchedFields - Set of touched field paths
 * @returns true if any touched path starts with the field path
 *
 * @example
 * const touched = new Set(["connectionConfiguration.tunnel_method.tunnel_user_password"]);
 * hasAnyTouchedNestedField("connectionConfiguration.tunnel_method", touched); // true
 * hasAnyTouchedNestedField("connectionConfiguration.host", touched); // false
 */
export const hasAnyTouchedNestedField = (fieldPath: string, touchedFields: Set<string>): boolean => {
  return Array.from(touchedFields).some((touchedPath) => touchedPath.startsWith(`${fieldPath}.`));
};

/**
 * Merges new configuration values with existing values while preserving touched fields.
 *
 * This function performs a deep merge, recursively traversing objects at any depth.
 * Any field path in the touchedFields set (or any child path beneath it) will be preserved
 * from the current values, preventing overwrites.
 *
 * @param basePath - The dot-notation path prefix (e.g., "connectionConfiguration")
 * @param newValue - The new configuration value to merge in
 * @param getCurrentValue - Function to get the current value at a given path
 * @param touchedFields - Set of dot-notation field paths that should be preserved
 * @returns The merged value with touched fields preserved
 *
 * @example
 * // Preserves password field when merging config
 * const merged = mergePreservingTouchedFields(
 *   "connectionConfiguration",
 *   { host: "localhost", password: "***" },
 *   (path) => getValues(path),
 *   new Set(["connectionConfiguration.password"])
 * );
 * // Result: { host: "localhost", password: "original_password" }
 *
 * @example
 * // Works with deeply nested fields
 * const merged = mergePreservingTouchedFields(
 *   "connectionConfiguration",
 *   { tunnel_method: { tunnel_host: "new-host", tunnel_user_password: "***" } },
 *   (path) => getValues(path),
 *   new Set(["connectionConfiguration.tunnel_method.tunnel_user_password"])
 * );
 * // Result: { tunnel_method: { tunnel_host: "new-host", tunnel_user_password: "original_password" } }
 */
export function mergePreservingTouchedFields(
  basePath: string,
  newValue: unknown,
  getCurrentValue: (path: string) => unknown,
  touchedFields: Set<string>
): unknown {
  // For primitives, arrays, null/undefined - use the new value directly
  if (typeof newValue !== "object" || newValue === null || Array.isArray(newValue)) {
    return newValue;
  }

  // For objects, merge recursively
  const currentValue = getCurrentValue(basePath) || {};
  const mergedValue = { ...(currentValue as Record<string, unknown>) };

  Object.entries(newValue as Record<string, unknown>).forEach(([key, value]) => {
    const fieldPath = `${basePath}.${key}`;

    // Skip if this exact field has been touched
    if (touchedFields.has(fieldPath)) {
      // Keep the current value, don't overwrite
      return;
    }

    // If this field contains nested touched fields, recursively merge
    if (hasAnyTouchedNestedField(fieldPath, touchedFields)) {
      mergedValue[key] = mergePreservingTouchedFields(fieldPath, value, getCurrentValue, touchedFields);
    } else {
      // Otherwise, use the new value
      mergedValue[key] = value;
    }
  });

  return mergedValue;
}
