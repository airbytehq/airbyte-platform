import { ExtendedJSONSchema, JSONSchemaExtension } from "json-schema-to-ts/lib/types/definitions";
import isBoolean from "lodash/isBoolean";
import { DefaultValues, FieldValues, get } from "react-hook-form";

export class AirbyteJsonSchemaExtention implements JSONSchemaExtension {
  [k: string]: unknown;
  multiline?: boolean;
  patternDescriptor?: string;
  linkable?: boolean;
  deprecated?: boolean;
}

export type AirbyteJsonSchema = Exclude<ExtendedJSONSchema<AirbyteJsonSchemaExtention>, boolean>;

/**
 * Extracts default values from the JSON schema
 */
export const extractDefaultValuesFromSchema = <T extends FieldValues>(schema: AirbyteJsonSchema): DefaultValues<T> => {
  if (schema.default !== undefined) {
    return schema.default as DefaultValues<T>;
  }

  if (schema.type === "array") {
    return [] as DefaultValues<T>;
  }

  if (schema.type === "string") {
    if (schema.enum && Array.isArray(schema.enum) && schema.enum.length >= 1) {
      return schema.enum[0] as DefaultValues<T>;
    }

    return "" as unknown as DefaultValues<T>;
  }

  if (schema.type === "number" || schema.type === "integer") {
    return null as unknown as DefaultValues<T>;
  }

  if (schema.oneOf || schema.anyOf) {
    const firstOptionSchema = (schema.oneOf ?? schema.anyOf)![0];
    if (firstOptionSchema && !isBoolean(firstOptionSchema)) {
      return extractDefaultValuesFromSchema(firstOptionSchema);
    }
  }

  if (schema.type !== "object" && !schema.properties) {
    return undefined as unknown as DefaultValues<T>;
  }

  const defaultValues: Record<string, unknown> = {};
  if (!schema.properties) {
    return defaultValues as DefaultValues<T>;
  }
  // Iterate through each property in the schema
  Object.entries(schema.properties).forEach(([key, property]) => {
    // ~ declarative_component_schema type handling ~
    const declarativeSchemaTypeValue = getDeclarativeSchemaTypeValue(key, property);
    if (declarativeSchemaTypeValue) {
      defaultValues[key] = declarativeSchemaTypeValue;
      return;
    }

    if (isBoolean(property)) {
      defaultValues[key] = property;
      return;
    }

    if (property.type === "object" && !schema.required?.includes(key)) {
      return;
    }

    const nestedDefaultValue = extractDefaultValuesFromSchema(property);
    if (nestedDefaultValue !== undefined && nestedDefaultValue !== null && !isEmptyObject(nestedDefaultValue)) {
      defaultValues[key] = nestedDefaultValue;
    }
  });

  return defaultValues as DefaultValues<T>;
};

export const getDeclarativeSchemaTypeValue = (
  propertyName: string,
  property: ExtendedJSONSchema<AirbyteJsonSchemaExtention>
) => {
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

export const isEmptyObject = (object: object): boolean => {
  return typeof object === "object" && !Array.isArray(object) && Object.keys(object).length === 0;
};

export const verifyArrayItems = (
  items:
    | ExtendedJSONSchema<AirbyteJsonSchemaExtention>
    | ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>
    | undefined,
  path: string
) => {
  if (!items) {
    throw new Error(`The items field of the array property at path ${path} must be defined.`);
  }
  if (Array.isArray(items)) {
    throw new Error(`The items field of the array property at path ${path} must not be an array.`);
  }
  if (isBoolean(items)) {
    throw new Error(`The items field of the array property at path ${path} must not be a boolean.`);
  }
  items = items as AirbyteJsonSchema;
  if (
    items.type !== "object" &&
    items.type !== "string" &&
    items.type !== "integer" &&
    items.type !== "number" &&
    !items.oneOf &&
    !items.anyOf
  ) {
    throw new Error(`Unsupported array item type: ${items.type} at path ${path}`);
  }
  return items;
};

export const getSelectedOptionSchema = (
  optionSchemas: ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>,
  value: unknown
): AirbyteJsonSchema | undefined => {
  if (value === undefined) {
    return undefined;
  }

  return optionSchemas.find((optionSchema) => {
    if (isBoolean(optionSchema)) {
      return false;
    }

    if (optionSchema.oneOf || optionSchema.anyOf) {
      return !!getSelectedOptionSchema((optionSchema.oneOf ?? optionSchema.anyOf)!, value);
    }

    if (value === null) {
      // treat null as empty value for number and integer types
      if (optionSchema.type === "number" || optionSchema.type === "integer") {
        return true;
      }
      return optionSchema.type === "null";
    }

    if (value === "") {
      if (optionSchema.type === "string") {
        return true;
      }
      return false;
    }

    if (typeof value === "object" && !("type" in value)) {
      return optionSchema.type === "object";
    }

    if (typeof value !== "object") {
      if (typeof value === "number" && (optionSchema.type === "integer" || optionSchema.type === "number")) {
        return true;
      }
      if (optionSchema.type === typeof value) {
        return true;
      }
      return false;
    }

    if (!optionSchema.properties) {
      if (optionSchema.additionalProperties) {
        return true;
      }
      return false;
    }

    // ~ declarative_component_schema type handling ~
    const discriminatorSchema = optionSchema.properties.type;

    if (!discriminatorSchema || isBoolean(discriminatorSchema) || discriminatorSchema.type !== "string") {
      return false;
    }
    if (
      !discriminatorSchema.enum ||
      !Array.isArray(discriminatorSchema.enum) ||
      discriminatorSchema.enum.length !== 1
    ) {
      return false;
    }

    if (!("type" in value)) {
      return false;
    }

    return value.type === discriminatorSchema.enum[0];
  }) as AirbyteJsonSchema | undefined;
};

export const getSchemaAtPath = (path: string, schema: AirbyteJsonSchema, data: FieldValues): AirbyteJsonSchema => {
  if (!path) {
    return schema;
  }

  const pathParts = path.split(".");
  let currentProperty = schema;
  let currentPath = "";

  for (const part of pathParts) {
    if (!Number.isNaN(Number(part)) && currentProperty.type === "array" && currentProperty.items) {
      // array path
      currentProperty = verifyArrayItems(currentProperty.items, currentPath);
      // Don't update currentPath here - will be updated at the end of the loop
    } else {
      let nextProperty: ExtendedJSONSchema<AirbyteJsonSchemaExtention>;
      const optionSchemas = currentProperty.oneOf ?? currentProperty.anyOf;

      if (optionSchemas && !currentProperty.properties) {
        // oneOf/anyOf path
        const selectedOption = getSelectedOptionSchema(optionSchemas, get(data, currentPath));
        if (!selectedOption) {
          throw new Error(`No matching schema found for path: ${currentPath}`);
        }
        if (!selectedOption.properties) {
          // debugger;
          if (selectedOption.additionalProperties && !isBoolean(selectedOption.additionalProperties)) {
            nextProperty = selectedOption.additionalProperties;
          } else {
            throw new Error(
              `Invalid schema path: ${currentPath}. All oneOf/anyOf options must have properties or additionalProperties object.`
            );
          }
        } else {
          nextProperty = selectedOption.properties[part];
        }
      } else if (!currentProperty.properties) {
        // Check if we have additionalProperties defined as an object schema
        if (
          typeof currentProperty.additionalProperties === "object" &&
          !Array.isArray(currentProperty.additionalProperties)
        ) {
          // For arbitrary keys, use the additionalProperties schema
          nextProperty = currentProperty.additionalProperties;
        } else {
          throw new Error(`Invalid schema path: ${path}. No properties found at subpath ${currentPath}`);
        }
      } else {
        // First try to find the property in the defined properties
        nextProperty = currentProperty.properties[part];

        // If property not found but additionalProperties is defined, use that schema
        if (
          !nextProperty &&
          typeof currentProperty.additionalProperties === "object" &&
          !Array.isArray(currentProperty.additionalProperties)
        ) {
          nextProperty = currentProperty.additionalProperties;
        }
      }

      if (!nextProperty) {
        throw new Error(`Invalid schema path: ${currentPath}. Property ${part} not found.`);
      }
      if (typeof nextProperty === "boolean") {
        throw new Error(`Invalid schema path: ${path}. Property ${part} is a boolean, not an object.`);
      }

      currentProperty = nextProperty;
    }
    // Always add the part to the currentPath - whether it's an array index or a property name
    currentPath = `${currentPath ? `${currentPath}.` : ""}${part}`;
  }

  return currentProperty;
};

// Convert a $ref of the form "#/path/to/field" to a path of the form "path.to.field"
export const convertRefToPath = (ref: string) => {
  return ref.replace("#/", "").replace(/\//g, ".");
};

/**
 * Dereferences all $ref occurrences in a JSON object by replacing them with the actual values
 * @param obj The object containing references to resolve
 * @param root The root object containing the reference targets (defaults to obj)
 * @param visitedRefs Set of already visited references to avoid circular references
 * @returns A new object with all references resolved
 */
export const resolveRefs = <T>(obj: T, root?: unknown, visitedRefs: Set<string> = new Set()): T => {
  if (!obj || typeof obj !== "object") {
    return obj;
  }

  const rootObj = root ?? obj;

  if (Array.isArray(obj)) {
    // For all arrays, give each item its own independent tracking of visited references
    // This prevents cross-contamination where a circular reference in one item
    // would block resolution of other items
    return obj.map((item) => resolveRefs(item, rootObj, new Set(visitedRefs))) as unknown as T;
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

    const path = convertRefToPath(ref);
    const target = get(rootObj, path);

    // If target doesn't exist, return the original object as-is
    if (target === undefined) {
      console.warn(`Reference not found: ${ref}`);
      return obj;
    }

    // Keep keys from the original object if they overlap with those in the resolved target,
    // as this is the only way to override titles and descriptions for $ref'd schemas.
    const resolvedTarget = resolveRefs(target, rootObj, visitedRefs);
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
    result[key] = resolveRefs(value, rootObj, new Set(visitedRefs));
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
