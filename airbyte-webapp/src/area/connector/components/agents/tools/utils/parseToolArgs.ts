/**
 * Safely parses tool call arguments which can be either a JSON string or an object.
 * Returns null if parsing fails or args is null.
 */
export const parseToolArgs = <T = Record<string, unknown>>(args: string | Record<string, unknown> | null): T | null => {
  if (args === null) {
    return null;
  }

  if (typeof args === "string") {
    try {
      return JSON.parse(args) as T;
    } catch (error) {
      console.error("Failed to parse tool call args:", error);
      return null;
    }
  }

  return args as T;
};
