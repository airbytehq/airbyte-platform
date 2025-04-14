import { ajvResolver } from "@hookform/resolvers/ajv";
import { JSONSchemaType } from "ajv";
import { fullFormats } from "ajv-formats/dist/formats";
import {
  DeepRequired,
  FieldError,
  FieldErrorsImpl,
  FieldErrors,
  FieldValues,
  Merge,
  ResolverOptions,
  get,
} from "react-hook-form";

import { NON_I18N_ERROR_TYPE, removeEmptyProperties } from "core/utils/form";

import {
  AirbyteJsonSchema,
  AirbyteJsonSchemaExtention,
  getSchemaAtPath,
  getSelectedOptionSchema,
  hasFields,
} from "./utils";

/**
 * Creates a schema validator with customized error message handling
 */
export const schemaValidator = <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>(
  schema: JsonSchema
) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return async (data: TsSchema, context: any, options: ResolverOptions<TsSchema>) => {
    // Remove empty properties to avoid validating those
    removeEmptyProperties(data);

    // Run the standard AJV validation
    const ajv = ajvResolver(schema as JSONSchemaType<unknown>, {
      coerceTypes: true,
      formats: fullFormats,
      keywords: Object.keys(new AirbyteJsonSchemaExtention()),
    });
    const result = await ajv(data, context, options);
    const errors = result.errors;

    // If there are no errors, return the result
    if (!hasFields(errors)) {
      return result;
    }

    // Otherwise, customize the error messages
    await overrideAjvErrorMessages(errors, schema, data, context, options);

    return result;
  };
};

/**
 * Customizes AJV error messages to be more user-friendly
 */
export const overrideAjvErrorMessages = async <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>(
  errors: FieldErrors<TsSchema>,
  schema: JsonSchema,
  data: TsSchema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  context: any,
  options: ResolverOptions<TsSchema>,
  path: string = ""
) => {
  const errorKeys = Object.keys(errors);
  // Find keys that are not standard error properties, as these represent nested errors
  const nonFieldErrorKeys = errorKeys.filter((key) => !["type", "message", "ref", "types"].includes(key));

  for (const key of nonFieldErrorKeys) {
    const errorPath = `${path ? `${path}.` : ""}${key}`;
    const error = errors[key];
    if (!error) {
      continue;
    }

    // Process nested errors first (depth-first approach)
    await overrideAjvErrorMessages(error as FieldErrors<TsSchema>, schema, data, context, options, errorPath);

    // Skip if this is not a leaf error node
    if (!error.type && !error.message && !error.ref) {
      continue;
    }

    // Mark typeless errors as non-i18n errors
    if (!error.type && error.message) {
      error.type = NON_I18N_ERROR_TYPE;
      continue;
    }

    // Handle standard error types
    if (error.type === "required") {
      error.message = "form.empty.error";
      continue;
    }

    // Special handling for oneOf/anyOf errors
    if (error.type === "oneOf" || error.type === "anyOf") {
      await handleMultiSchemaErrors(error, errorPath, schema, data, context, options);
      continue;
    }

    // For all other errors, ensure they don't trigger translation warnings
    error.type = NON_I18N_ERROR_TYPE;
    if (error.message) {
      error.message = String(error.message).charAt(0).toUpperCase() + String(error.message).slice(1);
    }
  }
};

/**
 * Helper function for handling oneOf/anyOf schema validation errors
 */
const handleMultiSchemaErrors = async <JsonSchema extends AirbyteJsonSchema, TsSchema extends FieldValues>(
  error: FieldError | Merge<FieldError, FieldErrorsImpl<DeepRequired<TsSchema>[string]>>,
  errorPath: string,
  schema: JsonSchema,
  data: TsSchema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  context: any,
  options: ResolverOptions<TsSchema>
) => {
  const multiOptionSchema = getSchemaAtPath(errorPath, schema, data);
  const optionSchemas = multiOptionSchema.oneOf ?? multiOptionSchema.anyOf;
  if (!optionSchemas) {
    return;
  }

  const valueAtPath = get(data, errorPath);
  const selectedOptionSchema = getSelectedOptionSchema(optionSchemas, valueAtPath) as JsonSchema;

  // If no schema is selected yet, show a "Required" error
  if (!selectedOptionSchema) {
    error.type = "required";
    error.message = "form.empty.error";
    return;
  }

  // Validate against the selected schema to get more specific errors
  const validate = schemaValidator<JsonSchema, TsSchema>(selectedOptionSchema);
  const result = await validate(valueAtPath, context, options);
  const subErrors = result.errors;

  if (!hasFields(subErrors)) {
    return;
  }

  // Replace the generic oneOf/anyOf error with the more specific sub-errors
  delete error.message;
  delete error.ref;
  delete error.type;
  delete error.types;

  // Spread subErrors into error
  Object.keys(subErrors).forEach((subKey) => {
    (error as Record<string, unknown>)[subKey] = (subErrors as Record<string, unknown>)[subKey];
  });
};
