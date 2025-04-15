import { createContext, useCallback, useContext } from "react";
import {
  FieldError,
  FieldValues,
  FormProvider,
  UseFormProps,
  get,
  useForm,
  useFormContext,
  useFormState,
} from "react-hook-form";

import {
  AirbyteJsonSchema,
  extractDefaultValuesFromSchema,
  getSchemaAtPath,
  getSelectedOptionSchema,
  schemaValidator,
} from "./utils";
import { FormSubmissionHandler } from "../Form";

interface SchemaFormProps<JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues> {
  schema: JsonSchema;
  onSubmit?: FormSubmissionHandler<TsSchema>;
  onSuccess?: (values: TsSchema) => void;
  onError?: (e: Error, values: TsSchema) => void;
  mode?: UseFormProps<TsSchema>["mode"];
}

export const SchemaForm = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>({
  children,
  schema,
  onSubmit,
  onSuccess,
  onError,
}: React.PropsWithChildren<SchemaFormProps<JsonSchema, TsSchema>>) => {
  const methods = useForm<TsSchema>({
    criteriaMode: "all",
    mode: "onChange",
    defaultValues: extractDefaultValuesFromSchema<TsSchema>(schema),
    resolver: schemaValidator(schema),
  });

  const processSubmission = (values: TsSchema) => {
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
  };

  if (!schema.properties) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <SchemaFormProvider schema={schema}>
        <form onSubmit={methods.handleSubmit(processSubmission)}>{children}</form>
      </SchemaFormProvider>
    </FormProvider>
  );
};

interface SchemaFormContextValue {
  schema: AirbyteJsonSchema;
  schemaAtPath: (path: string) => AirbyteJsonSchema;
  errorAtPath: (path: string) => FieldError | undefined;
  isRequiredField: (path: string) => boolean;
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
}
const SchemaFormProvider: React.FC<React.PropsWithChildren<SchemaFormProviderProps>> = ({ children, schema }) => {
  const { getValues } = useFormContext();
  const { errors } = useFormState();

  const schemaAtPath = useCallback(
    (path: string): AirbyteJsonSchema => getSchemaAtPath(path, schema, getValues()),
    [getValues, schema]
  );

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

  const isRequiredField = useCallback(
    (path: string) => {
      const pathParts = path.split(".");
      const fieldName = pathParts.pop() || "";
      const parentPath = pathParts.join(".");
      const parentProperty = parentPath ? schemaAtPath(parentPath) : schema;
      if (parentProperty.type === "array") {
        return true;
      }
      const parentOptionSchemas = parentProperty.oneOf ?? parentProperty.anyOf;
      if (parentOptionSchemas) {
        const selectedOption = getSelectedOptionSchema(parentOptionSchemas, getValues(parentPath));
        if (!selectedOption) {
          throw new Error(`No matching schema found for path: ${parentPath}`);
        }
        return !!selectedOption.required?.includes(fieldName);
      }
      return !!parentProperty.required?.includes(fieldName);
    },
    [schema, getValues, schemaAtPath]
  );

  return (
    <SchemaFormContext.Provider value={{ schema, schemaAtPath, errorAtPath, isRequiredField }}>
      {children}
    </SchemaFormContext.Provider>
  );
};
