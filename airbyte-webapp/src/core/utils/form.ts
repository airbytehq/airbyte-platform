/**
 * React-hook-form will set field values to empty string if they got touched by the user and to undefined if the field gets registered without use interaction.
 * In some cases this can be undesirable, e.g. when objects are compared or when the data is sent to the server.
 * This helper function recursively removes all properties that contain empty strings or undefined as value.
 *
 * In most cases it should be used by passing the values object from the form submit callback to this function before sending data to the server.
 *
 * Things to consider:
 * * This will mutate the object passed to it.
 * * This will remove empty strings even if the properties are required
 * * This will remove strings that are empty after trimming
 * * This will remove properties that are undefined
 * * This will not remove properties that are null
 */
export function removeEmptyProperties<T>(obj: T, removeNull = false) {
  if (typeof obj !== "object" || obj === null) {
    return obj;
  }
  if (Array.isArray(obj)) {
    obj.forEach((item) => removeEmptyProperties(item, removeNull));
    return obj;
  }
  for (const key in obj) {
    if (obj.hasOwnProperty(key)) {
      const val = (obj as Record<string, unknown>)[key];
      if (val === undefined || (typeof val === "string" && val.trim() === "") || (removeNull && val === null)) {
        delete (obj as Record<string, unknown>)[key];
      } else if (typeof val === "object") {
        removeEmptyProperties(val, removeNull);
      }
    }
  }
  return obj;
}

export const NON_I18N_ERROR_TYPE = "non-i18n-error";
