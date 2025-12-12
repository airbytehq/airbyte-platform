/**
 * Determines if a given field path corresponds to a secret field in the JSON schema.
 *
 * This function navigates through the schema following the field path and checks if
 * the final property is marked with `airbyte_secret: true`. It handles nested objects
 * and oneOf schemas.
 *
 * @param fieldPath - Dot-notation path like "connectionConfiguration.password"
 * @param schema - The full JSON schema (with connectionConfiguration property)
 * @returns true if the field is marked with airbyte_secret: true in any possible schema branch
 *
 * @example
 * const schema = {
 *   type: "object",
 *   properties: {
 *     connectionConfiguration: {
 *       type: "object",
 *       properties: {
 *         password: { type: "string", airbyte_secret: true },
 *       },
 *     },
 *   },
 * };
 * isSecretField("connectionConfiguration.password", schema); // returns true
 */
export const isSecretField = (fieldPath: string, schema: Record<string, unknown>): boolean => {
  // Return false for invalid inputs
  if (!fieldPath || !schema || typeof schema !== "object") {
    return false;
  }

  // Split path into segments
  const pathSegments = fieldPath.split(".");

  // Recursive helper to navigate schema
  const checkPath = (currentSchema: unknown, segments: string[], depth: number): boolean => {
    if (segments.length === 0) {
      return false;
    }

    if (typeof currentSchema !== "object" || currentSchema === null) {
      return false;
    }

    const schemaObj = currentSchema as Record<string, unknown>;

    // Handle oneOf at current level
    if (schemaObj.oneOf && Array.isArray(schemaObj.oneOf)) {
      // Check if ANY branch contains the field as a secret
      for (const branch of schemaObj.oneOf) {
        if (checkPath(branch, segments, depth)) {
          return true;
        }
      }
      return false;
    }

    // Get properties
    if (!schemaObj.properties || typeof schemaObj.properties !== "object") {
      return false;
    }

    const properties = schemaObj.properties as Record<string, unknown>;
    const [currentSegment, ...remainingSegments] = segments;

    // Get property schema for current segment
    const propertySchema = properties[currentSegment];
    if (!propertySchema || typeof propertySchema !== "object") {
      return false;
    }

    const propSchemaObj = propertySchema as Record<string, unknown>;

    // If this is the last segment, check for airbyte_secret
    if (remainingSegments.length === 0) {
      return propSchemaObj.airbyte_secret === true;
    }

    // Not the last segment, continue navigating
    // Check if property has oneOf
    if (propSchemaObj.oneOf && Array.isArray(propSchemaObj.oneOf)) {
      // Check all branches
      for (const branch of propSchemaObj.oneOf) {
        if (checkPath(branch, remainingSegments, depth + 1)) {
          return true;
        }
      }
      return false;
    }

    // Regular nested object
    if (propSchemaObj.type === "object" || propSchemaObj.properties) {
      return checkPath(propSchemaObj, remainingSegments, depth + 1);
    }

    return false;
  };

  return checkPath(schema, pathSegments, 0);
};
