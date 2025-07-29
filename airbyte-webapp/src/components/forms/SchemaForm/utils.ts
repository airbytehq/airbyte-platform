import { ExtendedJSONSchema, JSONSchemaExtension } from "json-schema-to-ts/lib/types/definitions";
import isBoolean from "lodash/isBoolean";
import { get } from "react-hook-form";

export class AirbyteJsonSchemaExtention implements JSONSchemaExtension {
  [k: string]: unknown;
  multiline?: boolean;
  pattern?: string;
  patternDescriptor?: string;
  linkable?: boolean;
  deprecated?: boolean;
  deprecation_message?: string;
  interpolation_context?: string[];
}

export type AirbyteJsonSchema = Exclude<ExtendedJSONSchema<AirbyteJsonSchemaExtention>, boolean>;

export const getDeclarativeSchemaTypeValue = (
  propertyName: string,
  property: ExtendedJSONSchema<AirbyteJsonSchemaExtention> | undefined
) => {
  if (!property) {
    return undefined;
  }
  if (propertyName !== "type") {
    return undefined;
  }
  if (isBoolean(property)) {
    return undefined;
  }
  if (!property.enum || !Array.isArray(property.enum) || property.enum.length !== 1) {
    return undefined;
  }
  return property.enum[0];
};

// eslint-disable-next-line @typescript-eslint/ban-types
export const hasFields = <T extends object>(object: T | {}): object is T => {
  return Object.keys(object).length > 0;
};

export const isEmptyObject = (object: unknown): boolean => {
  return typeof object === "object" && object !== null && !Array.isArray(object) && Object.keys(object).length === 0;
};

// Convert a $ref of the form "#/path/to/field" to a path of the form "path.to.field"
export const convertRefToPath = (ref: string, basePath?: string) => {
  const convertedPath = ref.replace("#/", "").replace(/\//g, ".");
  if (basePath) {
    return `${basePath}.${convertedPath}`;
  }
  return convertedPath;
};

// Convert a path of the form "path.to.field" to a $ref of the form "#/path/to/field"
export const convertPathToRef = (path: string, basePath?: string) => {
  const convertedRefValue = path.replace(/\./g, "/");
  if (basePath && convertedRefValue.startsWith(basePath)) {
    return `#${convertedRefValue.replace(basePath, "")}`;
  }
  return `#/${convertedRefValue}`;
};

/**
 * Dereferences all $ref occurrences in a JSON object by replacing them with the actual values
 * @param obj The object containing references to resolve
 * @param root The root object containing the reference targets (defaults to obj)
 * @param visitedRefs Set of already visited references to avoid circular references
 * @returns A new object with all references resolved
 */
export const resolveRefs = <T>(
  obj: T,
  root?: unknown,
  basePath?: string,
  refTargetPath?: string,
  visitedRefs: Set<string> = new Set()
): T => {
  if (!obj || typeof obj !== "object") {
    return obj;
  }

  const rootObj = root ?? obj;

  if (Array.isArray(obj)) {
    // For all arrays, give each item its own independent tracking of visited references
    // This prevents cross-contamination where a circular reference in one item
    // would block resolution of other items
    return obj.map((item) => resolveRefs(item, rootObj, basePath, refTargetPath, new Set(visitedRefs))) as unknown as T;
  }

  if ("$ref" in obj && typeof obj.$ref === "string") {
    const ref = obj.$ref;

    // If we've already visited this reference or if it's a circular reference,
    // return the original object as-is without attempting to resolve it
    if (visitedRefs.has(ref)) {
      return obj;
    }

    // Add this reference to visited set before resolving it
    visitedRefs.add(ref);

    const path = convertRefToPath(ref, basePath);
    if (refTargetPath && !path.startsWith(refTargetPath)) {
      return obj;
    }
    const target = get(rootObj, path);

    // If target doesn't exist, return the original object as-is
    if (target === undefined) {
      console.warn(`Reference not found: ${ref}`);
      return obj;
    }

    // Keep keys from the original object if they overlap with those in the resolved target,
    // as this is the only way to override titles and descriptions for $ref'd schemas.
    const resolvedTarget = resolveRefs(target, rootObj, basePath, refTargetPath, visitedRefs);

    if (typeof resolvedTarget !== "object") {
      return resolvedTarget;
    }

    const { $ref, ...withoutRef } = obj;
    const resolvedObj: Record<string, unknown> = { ...withoutRef };
    Object.entries(resolvedTarget).forEach(([key, value]) => {
      if (resolvedObj[key] === undefined) {
        resolvedObj[key] = value;
      }
    });

    // Resolve the target with the updated visited references set
    return resolvedObj as T;
  }

  const result = {} as Record<string, unknown>;

  // Process each object property with its own independent reference tracking
  // This prevents cross-contamination between properties
  for (const [key, value] of Object.entries(obj)) {
    const resolved = resolveRefs(value, rootObj, basePath, refTargetPath, new Set(visitedRefs));
    result[key] = resolved;
  }

  return result as T;
};

export const displayName = (path: string, title?: string) => {
  if (title !== undefined) {
    return title;
  }

  const fieldName = path.split(".").at(-1);

  if (fieldName === undefined) {
    return undefined;
  }

  // If the field name is not a number, format and display it
  if (Number.isNaN(Number(fieldName))) {
    return fieldName.split("_").map(capitalizeFirstLetter).join(" ");
  }

  // If the field name is a number, it is likely an array index
  // and we don't need to display that as a control label
  return undefined;
};

const capitalizeFirstLetter = (str: string) => {
  return str?.slice(0, 1).toUpperCase() + str?.slice(1);
};

export const resolveTopLevelRef = (
  rootSchema: AirbyteJsonSchema,
  currentSchema: AirbyteJsonSchema
): AirbyteJsonSchema => {
  if (currentSchema.$ref) {
    const path = convertRefToPath(currentSchema.$ref);
    const resolvedRef = get(rootSchema, path);
    const { $ref, ...withoutRef } = currentSchema;
    const resolvedObj: Record<string, unknown> = { ...withoutRef };
    Object.entries(resolvedRef).forEach(([key, value]) => {
      if (resolvedObj[key] === undefined) {
        resolvedObj[key] = value;
      }
    });
    return resolvedObj as AirbyteJsonSchema;
  }
  return currentSchema;
};

export const isAdvancedField = (fullFieldPath: string, objectPath: string, nonAdvancedFields?: string[]) => {
  if (!nonAdvancedFields) {
    return false;
  }
  const nonAdvancedFullPaths = nonAdvancedFields.map((field) => `${objectPath}.${field}`);
  return !nonAdvancedFullPaths.includes(fullFieldPath);
};

export const scrollFieldIntoView = (path: string) => {
  const field = document.querySelector(`[data-field-path="${path}"]`);
  if (field) {
    field.scrollIntoView({ behavior: "smooth", block: "center" });
  }
};
