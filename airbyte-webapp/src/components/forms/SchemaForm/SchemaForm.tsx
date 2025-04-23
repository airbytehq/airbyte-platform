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
import { useIntl } from "react-intl";

import { RefsHandlerProvider } from "./RefsHandler";
import { schemaValidator } from "./schemaValidation";
import {
  AirbyteJsonSchema,
  extractDefaultValuesFromSchema,
  getSchemaAtPath,
  getSelectedOptionSchema,
  resolveRefs,
} from "./utils";
import { FormSubmissionHandler } from "../Form";
interface SchemaFormProps<JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues> {
  schema: JsonSchema;
  onSubmit?: FormSubmissionHandler<TsSchema>;
  onSuccess?: (values: TsSchema) => void;
  onError?: (e: Error, values: TsSchema) => void;
  initialValues?: DefaultValues<TsSchema>;
  refTargetPath?: string;
}

export const SchemaForm = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>({
  children,
  schema,
  onSubmit,
  onSuccess,
  onError,
  initialValues,
  refTargetPath,
}: React.PropsWithChildren<SchemaFormProps<JsonSchema, TsSchema>>) => {
  const { formatMessage } = useIntl();
  const resolvedSchema = useMemo(() => resolveRefs(schema), [schema]);
  const rawStartingValues = useMemo(
    () => initialValues ?? extractDefaultValuesFromSchema<TsSchema>(resolvedSchema),
    [initialValues, resolvedSchema]
  );
  const resolvedStartingValues = useMemo(() => resolveRefs(rawStartingValues), [rawStartingValues]);
  const methods = useForm<TsSchema>({
    criteriaMode: "all",
    mode: "onChange",
    defaultValues: resolvedStartingValues,
    resolver: schemaValidator(resolvedSchema, formatMessage),
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

  if (!resolvedSchema.properties) {
    return null;
  }

  return (
    <FormProvider {...methods}>
      <SchemaFormProvider schema={resolvedSchema}>
        <RefsHandlerProvider values={rawStartingValues} refTargetPath={refTargetPath}>
          <form onSubmit={methods.handleSubmit(processSubmission)}>{children}</form>
        </RefsHandlerProvider>
      </SchemaFormProvider>
    </FormProvider>
  );
};

// Simple context type that forwards SchemaForm and useRefsHandler capabilities
interface SchemaFormContextValue {
  // Schema & validation
  schema: AirbyteJsonSchema;
  schemaAtPath: (path: string, extractMultiOptionSchema?: boolean) => AirbyteJsonSchema;
  errorAtPath: (path: string) => FieldError | undefined;
  isRequiredField: (path: string) => boolean;

  // Rendered paths tracking
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
}
const SchemaFormProvider: React.FC<React.PropsWithChildren<SchemaFormProviderProps>> = ({ children, schema }) => {
  const { getValues } = useFormContext();
  const { errors } = useFormState();

  // Use a ref instead of state for rendered paths to prevent temporarily rendering fields twice
  const renderedPathsRef = useRef<Set<string>>(new Set<string>());

  // Setup schema access
  const schemaAtPath = useCallback(
    (path: string, extractMultiOptionSchema = false): AirbyteJsonSchema =>
      getSchemaAtPath(path, schema, getValues(), extractMultiOptionSchema),
    [getValues, schema]
  );

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

  const isRequiredField = useCallback(
    (path: string) => {
      if (!path) {
        return true;
      }

      const pathParts = path.split(".");
      const fieldName = pathParts.at(-1) ?? "";
      const parentPath = pathParts.slice(0, -1).join(".");
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

  return (
    <SchemaFormContext.Provider
      value={{
        // SchemaForm capabilities
        schema,
        schemaAtPath,
        errorAtPath,
        isRequiredField,

        // Rendered paths tracking
        renderedPathsRef,
        registerRenderedPath,
        isPathRendered,
      }}
    >
      {children}
    </SchemaFormContext.Provider>
  );
};
