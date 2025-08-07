import { zodResolver } from "@hookform/resolvers/zod";
import isBoolean from "lodash/isBoolean";
import { FieldErrors, FieldValues, ResolverOptions, ResolverResult } from "react-hook-form";
import { IntlShape } from "react-intl";
import { z } from "zod";

import { FORM_PATTERN_ERROR } from "core/form/types";
import { NON_I18N_ERROR_TYPE } from "core/utils/form";
import { getPatternDescriptor } from "views/Connector/ConnectorForm/utils";

import { OverrideByObjectField } from "./Controls/types";
import { getSelectedOptionSchema, verifyArrayItems } from "./SchemaForm";
import { AirbyteJsonSchema, getDeclarativeSchemaTypeValue, resolveTopLevelRef } from "./utils";

// Produces a dynamic zod schema that resolves $refs only as far as needed to validate the current values.
export const dynamicValidator = <TSchema extends FieldValues>(
  rootSchema: AirbyteJsonSchema,
  formatMessage: IntlShape["formatMessage"],
  overrideByObjectField?: OverrideByObjectField
) => {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  return async (data: TSchema, context: any, options: ResolverOptions<TSchema>): Promise<ResolverResult<TSchema>> => {
    try {
      // Convert JSON schema to a usable Zod schema with preset error messages
      const zodSchema = convertJsonSchemaToZodSchema(
        rootSchema,
        rootSchema,
        formatMessage,
        false,
        overrideByObjectField
      );

      // Use zodResolver which is CSP-compliant
      const resolver = zodResolver(zodSchema);
      const result = await resolver(data, context, options);

      // Add NON_I18N_ERROR_TYPE to all errors to avoid missing translation console errors
      if (result.errors) {
        // Traverse all errors and set their type to NON_I18N_ERROR_TYPE
        const traverseErrors = (obj: Record<string, unknown>) => {
          Object.keys(obj).forEach((key) => {
            const value = obj[key];
            if (value && typeof value === "object") {
              if ("type" in value && "message" in value) {
                // This is an error object, set its type
                (value as { type: string }).type = NON_I18N_ERROR_TYPE;
              } else {
                // This is a nested object, traverse it
                traverseErrors(value as Record<string, unknown>);
              }
            }
          });
        };

        traverseErrors(result.errors as Record<string, unknown>);
      }

      return result;
    } catch (error) {
      console.error("Error in schema validation:", error);

      // Return empty errors if schema conversion or validation fails
      return {
        values: {} as TSchema,
        errors: {
          _form: {
            type: "validation",
            message: "Validation failed due to internal error. Please check console for details.",
          },
        } as unknown as FieldErrors<TSchema>,
      };
    }
  };
};

const REQUIRED_ERROR = "form.empty.error";
const INVALID_TYPE_ERROR = "form.invalid_type.error";
export const convertJsonSchemaToZodSchema = (
  rootSchema: AirbyteJsonSchema,
  schema: AirbyteJsonSchema,
  formatMessage: IntlShape["formatMessage"],
  isRequired: boolean,
  overrideByObjectField: OverrideByObjectField | undefined
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
        if (valueContainsRef(value)) {
          return;
        }
        const selectedSchema = getSelectedOptionSchema(optionSchemas, value, rootSchema);
        if (!selectedSchema) {
          if (isRequired) {
            // Add a custom error if no matching schema is found
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: formatMessage({ id: "form.multiOptionSelect.error" }),
            });
          }
          return;
        }
        const zodSchema = convertJsonSchemaToZodSchema(
          rootSchema,
          selectedSchema,
          formatMessage,
          isRequired,
          overrideByObjectField
        );
        try {
          const result = zodSchema.safeParse(value);
          if (!result.success) {
            result.error.issues.forEach((issue) => {
              ctx.addIssue(issue);
            });
          }
        } catch (error) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: formatMessage({ id: "form.failedValidation" }, { error: String(error) }),
          });
        }
      });
    }

    if (schema.type === "object" && !schema.properties && schema.additionalProperties === true) {
      return z.any().superRefine((value, ctx) => {
        if (valueContainsRef(value)) {
          return;
        }
        if (value === undefined || value === "") {
          if (isRequired) {
            ctx.addIssue({
              code: z.ZodIssueCode.custom,
              message: formatMessage({ id: REQUIRED_ERROR }),
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

    const properties = schema.properties;
    if (properties && (!schema.type || schema.type === "object")) {
      const declarativeSchemaType = getDeclarativeSchemaTypeValue("type", properties.type);
      const validationOverride = overrideByObjectField?.[declarativeSchemaType]?.validate;
      if (validationOverride) {
        return validationOverride;
      }

      return z.any().superRefine((value, ctx) => {
        if (valueContainsRef(value)) {
          return;
        }

        if (isRequired && !value) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: formatMessage({ id: REQUIRED_ERROR }),
          });
        }

        if (typeof value !== "object" || value === null || Array.isArray(value)) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: formatMessage({ id: INVALID_TYPE_ERROR }),
          });
        }

        const required = schema.required ?? [];

        const zodProperties = Object.fromEntries(
          Object.entries(properties).map(([propertyKey, propertyValue]) => {
            if (isBoolean(propertyValue)) {
              return [propertyKey, z.any()];
            }
            const isFieldRequired = required.includes(propertyKey);
            if (value[propertyKey] === undefined || value[propertyKey] === null || value[propertyKey] === "") {
              return [
                propertyKey,
                z.any().refine((_value) => (isFieldRequired ? false : true), {
                  message: formatMessage({ id: REQUIRED_ERROR }),
                }),
              ];
            }
            return [
              propertyKey,
              convertJsonSchemaToZodSchema(
                rootSchema,
                resolveTopLevelRef(rootSchema, propertyValue),
                formatMessage,
                isFieldRequired,
                overrideByObjectField
              ),
            ];
          })
        );

        const zodObject = z.object(zodProperties);
        try {
          const result = zodObject.safeParse(value);
          if (!result.success) {
            result.error.issues.forEach((issue) => {
              ctx.addIssue(issue);
            });
          }
        } catch (error) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            message: formatMessage({ id: "form.failedValidation" }, { error: String(error) }),
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
        zodNumber = zodNumber.int(formatMessage({ id: "form.invalidInteger" }));
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

      const zodNullableNumber = z.preprocess(
        (val) => (typeof val === "number" && isNaN(val) ? null : val),
        zodNumber.nullable()
      );

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
      const itemsSchema = verifyArrayItems(schema.items, rootSchema);
      const arraySchema = z.array(
        convertJsonSchemaToZodSchema(rootSchema, itemsSchema, formatMessage, isRequired, overrideByObjectField),
        {
          required_error: formatMessage({ id: REQUIRED_ERROR }),
          invalid_type_error: formatMessage({ id: INVALID_TYPE_ERROR }),
        }
      );

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
        return convertJsonSchemaToZodSchema(
          rootSchema,
          typeSchema as AirbyteJsonSchema,
          formatMessage,
          isRequired,
          overrideByObjectField
        );
      });

      if (typeSchemas.length >= 2) {
        return z.union(typeSchemas as [z.ZodTypeAny, z.ZodTypeAny, ...z.ZodTypeAny[]]);
      } else if (typeSchemas.length === 1) {
        return typeSchemas[0];
      }
    }

    if (
      schema.additionalProperties &&
      typeof schema.additionalProperties === "object" &&
      !Array.isArray(schema.additionalProperties)
    ) {
      // Use the additionalProperties schema for validation of arbitrary fields
      const additionalPropSchema = convertJsonSchemaToZodSchema(
        rootSchema,
        resolveTopLevelRef(rootSchema, schema.additionalProperties),
        formatMessage,
        true,
        overrideByObjectField
      );
      return z.object({}).catchall(additionalPropSchema);
    }

    // Fall back to a passthrough schema for anything we couldn't convert
    return z.any();
  } catch (error) {
    console.error("Error converting JSON schema to Zod schema:", error, schema);
    // Default to accepting any value if conversion fails
    return z.any();
  }
};

const valueContainsRef = (value: unknown) => {
  if (typeof value === "object" && value !== null && "$ref" in value) {
    return true;
  }
  return false;
};
