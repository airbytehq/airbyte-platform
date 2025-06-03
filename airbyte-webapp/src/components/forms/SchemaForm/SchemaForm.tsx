import { ExtendedJSONSchema } from "json-schema-to-ts";
import isBoolean from "lodash/isBoolean";
import { createContext, useCallback, useContext, useMemo, useRef } from "react";
import { DefaultValues, FieldValues, FormProvider, get, useForm, useFormContext } from "react-hook-form";

import { DynamicValidator } from "./DynamicValidator";
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

interface BaseSchemaFormProps<JsonSchema extends AirbyteJsonSchema> {
  schema: JsonSchema;
  refTargetPath?: string;
  onlyShowErrorIfTouched?: boolean;
  disableFormControls?: boolean;
  disableValidation?: boolean;
}

type RootSchemaFormProps<
  JsonSchema extends AirbyteJsonSchema,
  TsSchema extends FieldValues,
> = BaseSchemaFormProps<JsonSchema> & {
  onSubmit?: FormSubmissionHandler<TsSchema>;
  onSuccess?: (values: TsSchema) => void;
  onError?: (e: Error, values: TsSchema) => void;
  initialValues?: DefaultValues<TsSchema>;
};

const RootSchemaForm = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>({
  children,
  schema,
  onSubmit,
  onSuccess,
  onError,
  initialValues,
  refTargetPath,
  onlyShowErrorIfTouched,
  disableFormControls,
  disableValidation,
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
        .then(() => {
          onSuccess?.(values);
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
      <SchemaFormProvider
        schema={schema}
        onlyShowErrorIfTouched={onlyShowErrorIfTouched}
        disableFormControls={disableFormControls}
      >
        {!disableValidation && <DynamicValidator />}
        <RefsHandlerProvider values={rawStartingValues} refTargetPath={refTargetPath}>
          <form onSubmit={methods.handleSubmit(processSubmission)}>{children}</form>
        </RefsHandlerProvider>
      </SchemaFormProvider>
    </FormProvider>
  );
};

type NestedSchemaFormProps<JsonSchema extends AirbyteJsonSchema> = BaseSchemaFormProps<JsonSchema> & {
  nestedUnderPath: string;
};

const NestedSchemaForm = <JsonSchema extends AirbyteJsonSchema>({
  children,
  schema,
  nestedUnderPath,
  refTargetPath,
  onlyShowErrorIfTouched,
  disableFormControls,
  disableValidation,
}: React.PropsWithChildren<NestedSchemaFormProps<JsonSchema>>) => {
  const { getValues } = useFormContext();
  const rawStartingValues = useMemo(() => getValues(nestedUnderPath), [getValues, nestedUnderPath]);

  return (
    <SchemaFormProvider
      schema={schema}
      nestedUnderPath={nestedUnderPath}
      onlyShowErrorIfTouched={onlyShowErrorIfTouched}
      disableFormControls={disableFormControls}
    >
      {!disableValidation && <DynamicValidator nestedUnderPath={nestedUnderPath} />}
      <RefsHandlerProvider values={rawStartingValues} refTargetPath={refTargetPath}>
        {children}
      </RefsHandlerProvider>
    </SchemaFormProvider>
  );
};

interface SchemaFormContextValue {
  schema: AirbyteJsonSchema;
  onlyShowErrorIfTouched?: boolean;
  nestedUnderPath?: string;
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
  getSchemaAtPath: (path: string, resolveMultiOptionSchema?: boolean) => AirbyteJsonSchema;
  renderedPathsRef: React.MutableRefObject<Set<string>>;
  registerRenderedPath: (path: string) => void;
  isPathRendered: (path: string) => boolean;
  isRequired: (path: string) => boolean;
  disableFormControls?: boolean;
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
  onlyShowErrorIfTouched?: boolean;
  nestedUnderPath?: string;
  disableFormControls?: boolean;
}
const SchemaFormProvider: React.FC<React.PropsWithChildren<SchemaFormProviderProps>> = ({
  children,
  schema,
  onlyShowErrorIfTouched,
  nestedUnderPath,
  disableFormControls,
}) => {
  const { getValues } = useFormContext();
  // Use a ref instead of state for rendered paths to prevent temporarily rendering fields twice
  const renderedPathsRef = useRef<Set<string>>(new Set<string>());

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
    (path: string, resolveMultiOptionSchema?: boolean) =>
      getSchemaAtPath(
        path,
        schema,
        nestedUnderPath ? getValues(nestedUnderPath) : getValues(),
        nestedUnderPath,
        resolveMultiOptionSchema
      ),
    [schema, getValues, nestedUnderPath]
  );

  const isRequired = useCallback(
    (path: string) => {
      const pathParts = path.split(".");
      const fieldName = pathParts.at(-1);
      if (!fieldName) {
        return true;
      }
      const parentPath = pathParts.slice(0, -1).join(".");
      if (nestedUnderPath && nestedUnderPath.startsWith(parentPath)) {
        return true;
      }
      const parentSchema = getSchemaAtPathCallback(parentPath, true);
      if (parentSchema?.required?.includes(fieldName)) {
        return true;
      }

      return false;
    },
    [getSchemaAtPathCallback, nestedUnderPath]
  );

  return (
    <SchemaFormContext.Provider
      value={{
        schema,
        onlyShowErrorIfTouched,
        nestedUnderPath,
        extractDefaultValuesFromSchema: extractDefaultValuesFromSchemaCallback,
        verifyArrayItems: verifyArrayItemsCallback,
        getSelectedOptionSchema: getSelectedOptionSchemaCallback,
        getSchemaAtPath: getSchemaAtPathCallback,
        renderedPathsRef,
        registerRenderedPath,
        isPathRendered,
        isRequired,
        disableFormControls,
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

export const verifyArrayItems = (
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

  return optionSchemas
    .map((optionSchema) => resolveTopLevelRef(rootSchema, optionSchema as AirbyteJsonSchema))
    .find((optionSchema) => {
      return valueIsCompatibleWithSchema(value, optionSchema, rootSchema);
    });
};

const valueIsCompatibleWithSchema = (
  value: unknown,
  schema: ExtendedJSONSchema<AirbyteJsonSchemaExtention>,
  rootSchema: AirbyteJsonSchema
): boolean => {
  if (isBoolean(schema)) {
    return false;
  }

  if (schema.oneOf || schema.anyOf) {
    return !!getSelectedOptionSchema((schema.oneOf ?? schema.anyOf)!, value, rootSchema);
  }

  if (value === null) {
    // treat null as empty value for number and integer types
    if (schema.type === "number" || schema.type === "integer") {
      return true;
    }
    return schema.type === "null";
  }

  if (value === "") {
    if (schema.type === "string") {
      return true;
    }
    return false;
  }

  if (Array.isArray(value)) {
    if (schema.type !== "array") {
      return false;
    }
    if (value.length > 0) {
      const itemSchema = verifyArrayItems(schema.items, rootSchema);
      return valueIsCompatibleWithSchema(value[0], itemSchema, rootSchema);
    }
    return schema.type === "array";
  }

  if (typeof value === "object" && !("type" in value)) {
    return schema.type === "object";
  }

  if (typeof value !== "object") {
    if (typeof value === "number" && (schema.type === "integer" || schema.type === "number")) {
      return true;
    }
    if (schema.type === typeof value) {
      return true;
    }
    return false;
  }

  if (!schema.properties) {
    if (schema.additionalProperties) {
      return true;
    }
    return false;
  }

  // ~ declarative_component_schema type handling ~
  const discriminatorSchema = schema.properties.type;

  if (!discriminatorSchema || isBoolean(discriminatorSchema)) {
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
  nestedUnderPath?: string,
  resolveMultiOptionSchema?: boolean
): AirbyteJsonSchema => {
  const targetPath = unnestPath(path, nestedUnderPath);
  if (!targetPath) {
    return rootSchema;
  }

  const pathParts = targetPath.split(".");
  let currentProperty = rootSchema;
  let currentPath = "";

  for (const part of pathParts) {
    // resolve potentially nested oneOf/anyOf
    let optionSchemas = currentProperty.oneOf ?? currentProperty.anyOf;
    while (optionSchemas && !currentProperty.properties) {
      const selectedOption = getSelectedOptionSchema(optionSchemas, get(data, currentPath), rootSchema);
      if (!selectedOption) {
        return currentProperty;
      }
      currentProperty = resolveTopLevelRef(rootSchema, selectedOption);
      optionSchemas = currentProperty.oneOf ?? currentProperty.anyOf;
    }

    // handle array index path
    if (!Number.isNaN(Number(part)) && currentProperty.type === "array" && currentProperty.items) {
      currentProperty = resolveTopLevelRef(rootSchema, verifyArrayItems(currentProperty.items, rootSchema));
    } else {
      let nextProperty: ExtendedJSONSchema<AirbyteJsonSchemaExtention>;
      if (!currentProperty.properties || !currentProperty.properties[part]) {
        // Check if we have additionalProperties defined as an object schema
        if (
          typeof currentProperty.additionalProperties === "object" &&
          !Array.isArray(currentProperty.additionalProperties)
        ) {
          // For arbitrary keys, use the additionalProperties schema
          nextProperty = currentProperty.additionalProperties;
        } else if (currentProperty.additionalProperties !== false) {
          // Empty schema will cause no validation to be performed as desired
          return {};
        } else {
          throw new Error(
            `Invalid schema path: '${targetPath}'. No properties found or additionalProperties not allowed at subpath '${currentPath}'`
          );
        }
      } else {
        // First try to find the property in the defined properties
        nextProperty = currentProperty.properties[part];
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

  if ((currentProperty.oneOf || currentProperty.anyOf) && resolveMultiOptionSchema) {
    const selectedOptionSchema = getSelectedOptionSchema(
      currentProperty.oneOf ?? currentProperty.anyOf ?? [],
      get(data, currentPath),
      rootSchema
    );
    if (!selectedOptionSchema) {
      return currentProperty;
    }
    return selectedOptionSchema;
  }

  return currentProperty;
};
