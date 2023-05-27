/**
 * React-hook-form will set field values to empty string if they got touched by the user. In some cases this can be undesirable.
 * This helper function recursively removes all properties that contain empty strings.
 *
 * In most cases it should be used by passing the values object from the form submit callback to this function before sending data to the server.
 *
 * Things to consider:
 * * This will mutate the object passed to it.
 * * This will remove empty strings even if the properties are required
 * * This will remove strings that are empty after trimming
 */
export function removeEmptyStrings(obj: unknown) {
  if (typeof obj !== "object" || obj === null) {
    return obj;
  }
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      const val = (obj as Record<string, unknown>)[key];
      if (typeof val === "string" && val.trim() === "") {
        delete (obj as Record<string, unknown>)[key];
      } else if (typeof val === "object") {
        removeEmptyStrings(val);
      }
    }
  }
  return obj;
}
