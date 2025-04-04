import { ExtendedJSONSchema, JSONSchemaExtension } from "json-schema-to-ts/lib/types/definitions";
import isBoolean from "lodash/isBoolean";
import { DefaultValues, FieldValues, get } from "react-hook-form";

export { schemaValidator, overrideAjvErrorMessages } from "./schemaValidation";

export class AirbyteJsonSchemaExtention implements JSONSchemaExtension {
  [k: string]: unknown;
  multiline?: boolean;
  patternDescriptor?: string;
}

export type AirbyteJsonSchema = Exclude<ExtendedJSONSchema<AirbyteJsonSchemaExtention>, boolean>;

/**
 * Extracts default values from the JSON schema
 */
export const extractDefaultValuesFromSchema = <T extends FieldValues>(schema: AirbyteJsonSchema): DefaultValues<T> => {
  if (schema.default) {
    return schema.default as DefaultValues<T>;
  }

  if (schema.type === "array") {
    return [] as DefaultValues<T>;
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
    if (key === "type" && !isBoolean(property) && property.enum && Array.isArray(property.enum)) {
      const [firstEnumValue] = property.enum;
      defaultValues[key] = firstEnumValue;
      return;
    }

    if (isBoolean(property)) {
      defaultValues[key] = property;
      return;
    }

    const nestedDefaultValue = extractDefaultValuesFromSchema(property);
    if (nestedDefaultValue !== undefined && !isEmptyObject(nestedDefaultValue)) {
      defaultValues[key] = nestedDefaultValue;
    }
  });

  return defaultValues as DefaultValues<T>;
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
    | undefined
) => {
  if (!items) {
    throw new Error("The items field of an array property must be defined.");
  }
  if (Array.isArray(items)) {
    throw new Error("The items field of an array property must not be an array.");
  }
  if (isBoolean(items)) {
    throw new Error("The items field of an array property must not be a boolean.");
  }
  items = items as AirbyteJsonSchema;
  if (items.type !== "object" && items.type !== "string") {
    throw new Error(`Unsupported array item type: ${items.type}`);
  }
  return items;
};

export const getSelectedOptionSchema = (
  optionSchemas: ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>,
  value: unknown
) => {
  if (!value || typeof value !== "object") {
    return undefined;
  }

  return optionSchemas.find((optionSchema) => {
    if (isBoolean(optionSchema)) {
      return false;
    }
    if (!optionSchema.properties) {
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
      currentProperty = verifyArrayItems(currentProperty.items);
      // Don't update currentPath here - will be updated at the end of the loop
    } else {
      let nextProperty: ExtendedJSONSchema<AirbyteJsonSchemaExtention>;
      const optionSchemas = currentProperty.oneOf ?? currentProperty.anyOf;
      if (optionSchemas) {
        // oneOf/anyOf path
        const selectedOption = getSelectedOptionSchema(optionSchemas, get(data, currentPath));
        if (!selectedOption) {
          throw new Error(`No matching schema found for path: ${currentPath}`);
        }
        if (!selectedOption.properties) {
          throw new Error(`Invalid schema path: ${currentPath}. All oneOf options must have properties.`);
        }
        nextProperty = selectedOption.properties[part];
      } else {
        // object path
        if (!currentProperty.properties) {
          throw new Error(`Invalid schema path: ${path}. No properties found at subpath ${currentPath}`);
        }
        nextProperty = currentProperty.properties[part];
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
