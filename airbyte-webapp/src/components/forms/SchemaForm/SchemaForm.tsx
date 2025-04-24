import { ExtendedJSONSchema } from "json-schema-to-ts";
import isBoolean from "lodash/isBoolean";
import { createContext, useCallback, useContext, useMemo, useRef } from "react";
import {
  DefaultValues,
  FieldError,
  FieldValues,
  FormProvider,
  get,
  useForm,
  useFormContext,
  useFormState,
} from "react-hook-form";
import { IntlShape, useIntl } from "react-intl";
import { z } from "zod";

import { FORM_PATTERN_ERROR } from "core/form/types";
import { getPatternDescriptor } from "views/Connector/ConnectorForm/utils";

import { RefsHandlerProvider } from "./RefsHandler";
import {
  AirbyteJsonSchema,
  getDeclarativeSchemaTypeValue,
  resolveRefs,
  resolveTopLevelRef,
  isEmptyObject,
  AirbyteJsonSchemaExtention,
  unnestPath,
} from "./utils";
import { FormSubmissionHandler } from "../Form";

type SchemaFormProps<JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues> =
  | RootSchemaFormProps<JsonSchema, TsSchema>
  | NestedSchemaFormProps<JsonSchema>;
export const SchemaForm = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>(
  props: React.PropsWithChildren<SchemaFormProps<JsonSchema, TsSchema>>
) => {
  if ("nestedUnderPath" in props) {
    return <NestedSchemaForm {...props} />;
  }

  return <RootSchemaForm {...props} />;
};

interface RootSchemaFormProps<JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues> {
  schema: JsonSchema;
  onSubmit?: FormSubmissionHandler<TsSchema>;
  onSuccess?: (values: TsSchema) => void;
  onError?: (e: Error, values: TsSchema) => void;
  initialValues?: DefaultValues<TsSchema>;
  refTargetPath?: string;
}

const RootSchemaForm = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>({
  children,
  schema,
  onSubmit,
  onSuccess,
  onError,
  initialValues,
  refTargetPath,
}: React.PropsWithChildren<RootSchemaFormProps<JsonSchema, TsSchema>>) => {
  const rawStartingValues = useMemo(
    () => initialValues ?? extractDefaultValuesFromSchema<TsSchema>(schema, schema),
    [initialValues, schema]
  );
  const resolvedStartingValues = useMemo(() => resolveRefs(rawStartingValues), [rawStartingValues]);
  const methods = useForm<TsSchema>({
    criteriaMode: "all",
    mode: "onChange",
    defaultValues: resolvedStartingValues,
  });

  const processSubmission = useCallback(
    (values: TsSchema) => {
      if (!onSubmit) {
        return;
      }

      return onSubmit(values, methods)
        .then((submissionResult) => {
          onSuccess?.(values);
          if (submissionResult) {
            methods.reset(submissionResult.resetValues ?? values, submissionResult.keepStateOptions ?? undefined);
          } else {
            methods.reset(values);
          }
        })
        .catch((e) => {
          onError?.(e, values);
        });
    },
    [onSubmit, onSuccess, onError, methods]
  );

  if (!schema.properties) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <SchemaFormProvider schema={schema}>
        <RefsHandlerProvider values={rawStartingValues} refTargetPath={refTargetPath}>
          <form onSubmit={methods.handleSubmit(processSubmission)}>{children}</form>
        </RefsHandlerProvider>
      </SchemaFormProvider>
    </FormProvider>
  );
};

interface NestedSchemaFormProps<JsonSchema extends AirbyteJsonSchema> {
  schema: JsonSchema;
  nestedUnderPath: string;
  refTargetPath?: string;
}

const NestedSchemaForm = <JsonSchema extends AirbyteJsonSchema>({
  children,
  schema,
  nestedUnderPath,
  refTargetPath,
}: React.PropsWithChildren<NestedSchemaFormProps<JsonSchema>>) => {
  const { getValues } = useFormContext();
  const rawStartingValues = useMemo(() => getValues(nestedUnderPath), [getValues, nestedUnderPath]);

  return (
    <SchemaFormProvider schema={schema} nestedUnderPath={nestedUnderPath}>
      <RefsHandlerProvider values={rawStartingValues} refTargetPath={refTargetPath}>
        {children}
      </RefsHandlerProvider>
    </SchemaFormProvider>
  );
};

interface SchemaFormContextValue {
  schema: AirbyteJsonSchema;
  nestedUnderPath?: string;
  errorAtPath: (path: string) => FieldError | undefined;
  extractDefaultValuesFromSchema: <T extends FieldValues>(fieldSchema: AirbyteJsonSchema) => DefaultValues<T>;
  verifyArrayItems: (
    items:
      | ExtendedJSONSchema<AirbyteJsonSchemaExtention>
      | ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>
      | undefined
  ) => AirbyteJsonSchema;
  getSelectedOptionSchema: (
    optionSchemas: ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>,
    value: unknown
  ) => AirbyteJsonSchema | undefined;
  getSchemaAtPath: (path: string, data: FieldValues) => AirbyteJsonSchema;
  convertJsonSchemaToZodSchema: (schema: AirbyteJsonSchema, isRequired: boolean) => z.ZodTypeAny;
  renderedPathsRef: React.MutableRefObject<Set<string>>;
  registerRenderedPath: (path: string) => void;
  isPathRendered: (path: string) => boolean;
}
const SchemaFormContext = createContext<SchemaFormContextValue | undefined>(undefined);
export const useSchemaForm = () => {
  const context = useContext(SchemaFormContext);
  if (!context) {
    throw new Error("useSchemaForm must be used within a SchemaFormProvider");
  }
  return context;
};

interface SchemaFormProviderProps {
  schema: AirbyteJsonSchema;
  nestedUnderPath?: string;
}
const SchemaFormProvider: React.FC<React.PropsWithChildren<SchemaFormProviderProps>> = ({
  children,
  schema,
  nestedUnderPath,
}) => {
  const { formatMessage } = useIntl();
  const { errors } = useFormState();

  // Use a ref instead of state for rendered paths to prevent temporarily rendering fields twice
  const renderedPathsRef = useRef<Set<string>>(new Set<string>());

  // Setup validation functions
  const errorAtPath = (path: string): FieldError | undefined => {
    const error: FieldError | undefined = get(errors, path);
    if (!error?.message) {
      return undefined;
    }
    return {
      type: error.type,
      message: error.message,
      ref: error.ref,
      types: error.types,
      root: error.root,
    };
  };

  // Update rendered paths tracking functions to use ref
  const registerRenderedPath = useCallback((path: string) => {
    if (path) {
      renderedPathsRef.current.add(path);
    }
  }, []);

  const isPathRendered = useCallback((path: string): boolean => {
    if (renderedPathsRef.current.has(path)) {
      return true;
    }

    // Check if any parent path has been rendered
    const pathParts = path.split(".");
    for (let i = 1; i < pathParts.length; i++) {
      const ancestorPath = pathParts.slice(0, i).join(".");
      if (renderedPathsRef.current.has(ancestorPath)) {
        return true;
      }
    }

    return false;
  }, []);

  const extractDefaultValuesFromSchemaCallback = useCallback(
    <T extends FieldValues>(fieldSchema: AirbyteJsonSchema) => extractDefaultValuesFromSchema<T>(fieldSchema, schema),
    [schema]
  );

  const verifyArrayItemsCallback = useCallback(
    (
      items:
        | ExtendedJSONSchema<AirbyteJsonSchemaExtention>
        | ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>
        | undefined
    ) => {
      return verifyArrayItems(items, schema);
    },
    [schema]
  );

  const getSelectedOptionSchemaCallback = useCallback(
    (optionSchemas: ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>, value: unknown) => {
      return getSelectedOptionSchema(optionSchemas, value, schema);
    },
    [schema]
  );

  const getSchemaAtPathCallback = useCallback(
    (path: string, data: FieldValues) => getSchemaAtPath(path, schema, data, nestedUnderPath),
    [schema, nestedUnderPath]
  );

  const convertJsonSchemaToZodSchemaCallback = useCallback(
    (jsonSchema: AirbyteJsonSchema, isRequired: boolean) =>
      convertJsonSchemaToZodSchema(schema, jsonSchema, formatMessage, isRequired),
    [schema, formatMessage]
  );

  return (
    <SchemaFormContext.Provider
      value={{
        schema,
        nestedUnderPath,
        errorAtPath,
        extractDefaultValuesFromSchema: extractDefaultValuesFromSchemaCallback,
        verifyArrayItems: verifyArrayItemsCallback,
        getSelectedOptionSchema: getSelectedOptionSchemaCallback,
        getSchemaAtPath: getSchemaAtPathCallback,
        convertJsonSchemaToZodSchema: convertJsonSchemaToZodSchemaCallback,
        renderedPathsRef,
        registerRenderedPath,
        isPathRendered,
      }}
    >
      {children}
    </SchemaFormContext.Provider>
  );
};

const extractDefaultValuesFromSchema = <T extends FieldValues>(
  fieldSchema: AirbyteJsonSchema,
  rootSchema: AirbyteJsonSchema
): DefaultValues<T> => {
  const resolvedSchema = resolveTopLevelRef(rootSchema, fieldSchema);

  if (resolvedSchema.default !== undefined) {
    return resolvedSchema.default as DefaultValues<T>;
  }

  if (resolvedSchema.type === "array") {
    const itemSchema = verifyArrayItems(resolvedSchema.items, rootSchema);
    if (itemSchema.type === "array") {
      return [extractDefaultValuesFromSchema(itemSchema, rootSchema)] as DefaultValues<T>;
    }
    return [] as DefaultValues<T>;
  }

  if (resolvedSchema.type === "string") {
    if (resolvedSchema.enum && Array.isArray(resolvedSchema.enum) && resolvedSchema.enum.length >= 1) {
      return resolvedSchema.enum[0] as DefaultValues<T>;
    }

    return "" as unknown as DefaultValues<T>;
  }

  if (resolvedSchema.type === "number" || resolvedSchema.type === "integer") {
    return null as unknown as DefaultValues<T>;
  }

  if (resolvedSchema.oneOf || resolvedSchema.anyOf) {
    const firstOptionSchema = (resolvedSchema.oneOf ?? resolvedSchema.anyOf)![0];
    if (firstOptionSchema && !isBoolean(firstOptionSchema)) {
      return extractDefaultValuesFromSchema(firstOptionSchema, rootSchema);
    }
  }

  if (resolvedSchema.type !== "object" && !resolvedSchema.properties) {
    return undefined as unknown as DefaultValues<T>;
  }

  const defaultValues: Record<string, unknown> = {};
  if (!resolvedSchema.properties) {
    return defaultValues as DefaultValues<T>;
  }
  // Iterate through each property in the schema
  Object.entries(resolvedSchema.properties).forEach(([key, property]) => {
    const resolvedProperty = resolveTopLevelRef(rootSchema, property as AirbyteJsonSchema);

    if (isBoolean(resolvedProperty)) {
      defaultValues[key] = resolvedProperty;
      return;
    }

    // ~ declarative_component_schema type handling ~
    const declarativeSchemaTypeValue = getDeclarativeSchemaTypeValue(key, property);
    if (declarativeSchemaTypeValue) {
      defaultValues[key] = declarativeSchemaTypeValue;
      return;
    }

    if (!resolvedProperty.default && !resolvedSchema.required?.includes(key)) {
      return;
    }

    const nestedDefaultValue = extractDefaultValuesFromSchema(resolvedProperty, rootSchema);
    if (nestedDefaultValue !== undefined && nestedDefaultValue !== null && !isEmptyObject(nestedDefaultValue)) {
      defaultValues[key] = nestedDefaultValue;
    }
  });

  return defaultValues as DefaultValues<T>;
};

const verifyArrayItems = (
  items:
    | ExtendedJSONSchema<AirbyteJsonSchemaExtention>
    | ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>
    | undefined,
  rootSchema: AirbyteJsonSchema
) => {
  if (!items) {
    throw new Error(`The "items" field of array properties must be defined.`);
  }
  if (Array.isArray(items)) {
    throw new Error(`The "items" field of array properties must not be an array.`);
  }
  if (isBoolean(items)) {
    throw new Error(`The "items" field of array properties must not be a boolean.`);
  }
  const resolvedItems = resolveTopLevelRef(rootSchema, items as AirbyteJsonSchema);
  if (
    resolvedItems.type !== "object" &&
    resolvedItems.type !== "array" &&
    resolvedItems.type !== "string" &&
    resolvedItems.type !== "integer" &&
    resolvedItems.type !== "number" &&
    !resolvedItems.oneOf &&
    !resolvedItems.anyOf
  ) {
    throw new Error(`Unsupported array "items" type: ${resolvedItems.type}`);
  }
  return resolvedItems;
};

export const getSelectedOptionSchema = (
  optionSchemas: ReadonlyArray<ExtendedJSONSchema<AirbyteJsonSchemaExtention>>,
  value: unknown,
  rootSchema: AirbyteJsonSchema
): AirbyteJsonSchema | undefined => {
  if (value === undefined) {
    return undefined;
  }

  return optionSchemas.find((optionSchema) => {
    return valueIsCompatibleWithSchema(value, optionSchema, rootSchema);
  }) as AirbyteJsonSchema | undefined;
};

const valueIsCompatibleWithSchema = (
  value: unknown,
  schema: ExtendedJSONSchema<AirbyteJsonSchemaExtention>,
  rootSchema: AirbyteJsonSchema
): boolean => {
  const resolvedSchema = resolveTopLevelRef(rootSchema, schema as AirbyteJsonSchema);
  if (isBoolean(resolvedSchema)) {
    return false;
  }

  if (resolvedSchema.oneOf || resolvedSchema.anyOf) {
    return !!getSelectedOptionSchema((resolvedSchema.oneOf ?? resolvedSchema.anyOf)!, value, rootSchema);
  }

  if (value === null) {
    // treat null as empty value for number and integer types
    if (resolvedSchema.type === "number" || resolvedSchema.type === "integer") {
      return true;
    }
    return resolvedSchema.type === "null";
  }

  if (value === "") {
    if (resolvedSchema.type === "string") {
      return true;
    }
    return false;
  }

  if (Array.isArray(value)) {
    if (resolvedSchema.type !== "array") {
      return false;
    }
    if (value.length > 0) {
      const itemSchema = verifyArrayItems(resolvedSchema.items, rootSchema);
      return valueIsCompatibleWithSchema(value[0], itemSchema, rootSchema);
    }
    return resolvedSchema.type === "array";
  }

  if (typeof value === "object" && !("type" in value)) {
    return resolvedSchema.type === "object";
  }

  if (typeof value !== "object") {
    if (typeof value === "number" && (resolvedSchema.type === "integer" || resolvedSchema.type === "number")) {
      return true;
    }
    if (resolvedSchema.type === typeof value) {
      return true;
    }
    return false;
  }

  if (!resolvedSchema.properties) {
    if (resolvedSchema.additionalProperties) {
      return true;
    }
    return false;
  }

  // ~ declarative_component_schema type handling ~
  const discriminatorSchema = resolvedSchema.properties.type;

  if (!discriminatorSchema || isBoolean(discriminatorSchema) || discriminatorSchema.type !== "string") {
    return false;
  }
  if (!discriminatorSchema.enum || !Array.isArray(discriminatorSchema.enum) || discriminatorSchema.enum.length !== 1) {
    return false;
  }

  if (!("type" in value)) {
    return false;
  }

  return value.type === discriminatorSchema.enum[0];
};

export const getSchemaAtPath = (
  path: string,
  rootSchema: AirbyteJsonSchema,
  data: FieldValues,
  nestedUnderPath?: string
): AirbyteJsonSchema => {
  const targetPath = unnestPath(path, nestedUnderPath);
  if (!targetPath) {
    return rootSchema;
  }

  const pathParts = targetPath.split(".");
  let currentProperty = rootSchema;
  let currentPath = "";

  for (const part of pathParts) {
    if (!Number.isNaN(Number(part)) && currentProperty.type === "array" && currentProperty.items) {
      // array path
      currentProperty = resolveTopLevelRef(rootSchema, verifyArrayItems(currentProperty.items, rootSchema));
      // Don't update currentPath here - will be updated at the end of the loop
    } else {
      let nextProperty: ExtendedJSONSchema<AirbyteJsonSchemaExtention>;
      const optionSchemas = currentProperty.oneOf ?? currentProperty.anyOf;

      if (optionSchemas && !currentProperty.properties) {
        // oneOf/anyOf path
        const selectedOption = getSelectedOptionSchema(optionSchemas, get(data, currentPath), rootSchema);
        if (!selectedOption) {
          throw new Error(`No matching schema found for path: ${currentPath}`);
        }
        if (!selectedOption.properties) {
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
          throw new Error(`Invalid schema path: ${targetPath}. No properties found at subpath ${currentPath}`);
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
        throw new Error(`Invalid schema path: ${targetPath}. Property ${part} is a boolean, not an object.`);
      }

      currentProperty = resolveTopLevelRef(rootSchema, nextProperty);
    }
    // Always add the part to the currentPath - whether it's an array index or a property name
    currentPath = `${currentPath ? `${currentPath}.` : ""}${part}`;
  }

  return currentProperty;
};

const REQUIRED_ERROR = "form.empty.error";
const INVALID_TYPE_ERROR = "form.invalid_type.error";
export const convertJsonSchemaToZodSchema = (
  rootSchema: AirbyteJsonSchema,
  schema: AirbyteJsonSchema,
  formatMessage: IntlShape["formatMessage"],
  isRequired: boolean
): z.ZodTypeAny => {
  try {
    // Handle enum values for any schema type
    if (schema.enum && Array.isArray(schema.enum)) {
      const enumSchema = z.enum(schema.enum as [string, ...string[]]);
      if (!isRequired) {
        return enumSchema.optional();
      }
      return enumSchema;
    }

    // Handle oneOf/anyOf as union types with refinement
    if (!schema.properties && (schema.oneOf || schema.anyOf)) {
      const optionSchemas = (schema.oneOf || schema.anyOf || []) as AirbyteJsonSchema[];
      if (optionSchemas.length === 0) {
        return z.any();
      }

      return z.any().superRefine((value, ctx) => {
        const selectedSchema = getSelectedOptionSchema(optionSchemas, value, rootSchema);
        if (!selectedSchema) {
          if (isRequired) {
            // Add a custom error if no matching schema is found
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: formatMessage({ id: "form.multiOptionSelect.error" }),
            });
          }
        }
      });
    }

    if (schema.type === "object" && !schema.properties && schema.additionalProperties === true) {
      return z.any().superRefine((value, ctx) => {
        if (value === undefined || value === "") {
          if (isRequired) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: formatMessage({ id: "form.empty.error" }),
            });
          }
        } else if (typeof value !== "object" || value === null || Array.isArray(value)) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: formatMessage({ id: "form.invalidJson" }),
          });
        }
      });
    }

    // For simple schemas, we can build them manually
    if (schema.type === "string") {
      // Create a base string schema with custom required error message
      let zodString = z.string({
        required_error: formatMessage({ id: REQUIRED_ERROR }),
        invalid_type_error: formatMessage({ id: INVALID_TYPE_ERROR }),
      });

      // Add string-specific validations with custom error messages
      if (schema.minLength) {
        zodString = zodString.min(schema.minLength, {
          message: formatMessage({ id: "form.minLength.error" }, { min: schema.minLength }),
        });
      }
      if (schema.maxLength) {
        zodString = zodString.max(schema.maxLength, {
          message: formatMessage({ id: "form.maxLength.error" }, { max: schema.maxLength }),
        });
      }
      if (schema.pattern) {
        zodString = zodString.regex(new RegExp(schema.pattern), {
          message: formatMessage(
            { id: FORM_PATTERN_ERROR },
            { pattern: getPatternDescriptor(schema) ?? schema.pattern }
          ),
        });
      }
      if (schema.format === "email") {
        zodString = zodString.email({ message: formatMessage({ id: "form.email.error" }) });
      }
      if (schema.format === "uri") {
        zodString = zodString.url({ message: formatMessage({ id: "form.url.error" }) });
      }

      if (!isRequired) {
        return zodString.optional();
      }
      return zodString.refine((value) => value !== "", {
        message: formatMessage({ id: REQUIRED_ERROR }),
      });
    } else if (schema.type === "number" || schema.type === "integer") {
      // Create a base number schema with custom error messages
      let zodNumber = z.number({
        required_error: formatMessage({ id: REQUIRED_ERROR }),
        invalid_type_error: formatMessage({ id: "form.invalidNumber" }),
      });

      if (schema.type === "integer") {
        zodNumber = zodNumber.int();
      }

      // Add number-specific validations with custom error messages
      if (schema.minimum !== undefined) {
        zodNumber = zodNumber.min(schema.minimum, {
          message: formatMessage({ id: "form.min.error" }, { min: schema.minimum }),
        });
      }
      if (schema.maximum !== undefined) {
        zodNumber = zodNumber.max(schema.maximum, {
          message: formatMessage({ id: "form.max.error" }, { max: schema.maximum }),
        });
      }

      const zodNullableNumber = zodNumber.nullable();

      if (!isRequired) {
        return zodNullableNumber.optional();
      }
      return zodNullableNumber.refine((value) => value !== null, {
        message: formatMessage({ id: REQUIRED_ERROR }),
      });
    } else if (schema.type === "boolean") {
      const zodBoolean = z.boolean({
        required_error: formatMessage({ id: REQUIRED_ERROR }),
        invalid_type_error: formatMessage({ id: INVALID_TYPE_ERROR }),
      });

      if (!isRequired) {
        return zodBoolean.optional();
      }
      return zodBoolean;
    } else if (schema.type === "null") {
      const zodNull = z.null();

      if (!isRequired) {
        return zodNull.optional();
      }
      return zodNull;
    } else if (schema.type === "array" && schema.items) {
      const arraySchema = z.array(z.any());

      // Add array-specific validations with custom error messages
      if (schema.minItems) {
        return arraySchema.min(schema.minItems, {
          message: formatMessage({ id: "form.minItems.error" }, { min: schema.minItems }),
        });
      }
      if (schema.maxItems) {
        return arraySchema.max(schema.maxItems, {
          message: formatMessage({ id: "form.maxItems.error" }, { max: schema.maxItems }),
        });
      }

      if (!isRequired) {
        return arraySchema.optional();
      }
      return arraySchema;
    }

    // Handle mixed types (e.g., ["string", "number"])
    if (Array.isArray(schema.type)) {
      // Create a union of the types
      const typeSchemas = schema.type.map((type) => {
        const typeSchema = { ...schema, type };
        return convertJsonSchemaToZodSchema(rootSchema, typeSchema as AirbyteJsonSchema, formatMessage, isRequired);
      });

      if (typeSchemas.length >= 2) {
        return z.union(typeSchemas as [z.ZodTypeAny, z.ZodTypeAny, ...z.ZodTypeAny[]]);
      } else if (typeSchemas.length === 1) {
        return typeSchemas[0];
      }
    }

    // Fall back to a passthrough schema for anything we couldn't convert
    return z.any();
  } catch (error) {
    console.error("Error converting JSON schema to Zod schema:", error, schema);
    // Default to accepting any value if conversion fails
    return z.any();
  }
};
