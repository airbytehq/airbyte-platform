import { useCallback, useEffect, useRef } from "react";
import { FieldValues, get, useFormContext, useWatch } from "react-hook-form";
import { IntlShape, useIntl } from "react-intl";
import { useMount } from "react-use";
import { z } from "zod";

import { FORM_PATTERN_ERROR } from "core/form/types";
import { getPatternDescriptor } from "views/Connector/ConnectorForm/utils";

import { getSelectedOptionSchema, useSchemaForm } from "./SchemaForm";
import { AirbyteJsonSchema, getDeclarativeSchemaTypeValue, isEmptyObject, resolveTopLevelRef } from "./utils";

/**
 * Dynamically determines all paths that need to be validated based on the form values.
 *
 * When a new path is added, its validation function is registered.
 *
 * When a path is removed, its validation function is unregistered.
 */
export const DynamicValidator = ({ nestedUnderPath }: { nestedUnderPath?: string }) => {
  const { formatMessage } = useIntl();
  const { getValues, watch, register, unregister } = useFormContext();
  const { schema: rootSchema, getSchemaAtPath, isRequired: isPathRequired } = useSchemaForm();
  const registeredPaths = useRef(new Set<string>());
  const values = useWatch();

  const registerValidation = useCallback(
    (path: string) => {
      const isRequired = isPathRequired(path);
      const targetSchema = resolveTopLevelRef(rootSchema, getSchemaAtPath(path, true));
      if (isEmptyObject(targetSchema)) {
        return;
      }
      register(path, {
        validate: (value) => {
          // ~ declarative_component_schema type handling ~
          if (getDeclarativeSchemaTypeValue(path.split(".").at(-1) ?? path, targetSchema)) {
            return true;
          }
          const zodSchema = convertJsonSchemaToZodSchema(rootSchema, targetSchema, formatMessage, isRequired);
          const result = zodSchema.safeParse(value);
          if (result.success === false) {
            return result.error.issues.at(-1)?.message;
          }
          return true;
        },
      });
      registeredPaths.current.add(path);
    },
    [rootSchema, getSchemaAtPath, isPathRequired, formatMessage, register]
  );

  useMount(() => {
    const allValues = getValues();
    const values = nestedUnderPath ? get(allValues, nestedUnderPath) : allValues;
    const paths = getAllFieldPaths(values, nestedUnderPath);
    for (const path of paths) {
      registerValidation(path);
    }
  });

  useEffect(() => {
    const subscription = watch((data, { name }) => {
      if (!name) {
        return;
      }
      if (nestedUnderPath && !name.startsWith(nestedUnderPath)) {
        return;
      }

      const oldValue = get(values, name);
      const updatedValue = get(data, name);
      if (typeof updatedValue === "object" || typeof oldValue === "object") {
        const oldSubPaths = new Set(getAllFieldPaths(get(values, name), name));
        const newSubPaths = new Set(getAllFieldPaths(updatedValue, name));
        const oldAndNewSubPaths = new Set([...oldSubPaths, ...newSubPaths]);
        for (const path of oldAndNewSubPaths) {
          const inOld = oldSubPaths.has(path);
          const inNew = newSubPaths.has(path);

          if (!inOld && inNew) {
            registerValidation(path);
          }
          if (inOld && !inNew) {
            unregister(path, { keepValue: true });
            registeredPaths.current.delete(path);
          }
        }
      }

      if (updatedValue === undefined) {
        unregister(name, { keepValue: true });
        registeredPaths.current.delete(name);
      } else {
        registerValidation(name);
      }
    });
    return () => subscription.unsubscribe();
  }, [watch, registerValidation, unregister, nestedUnderPath, getValues, values]);

  return null;
};

const REQUIRED_ERROR = "form.empty.error";
const INVALID_TYPE_ERROR = "form.invalid_type.error";
const convertJsonSchemaToZodSchema = (
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
          return;
        }
        if (selectedSchema.type !== "object" && selectedSchema.type !== "array") {
          const zodSchema = convertJsonSchemaToZodSchema(rootSchema, selectedSchema, formatMessage, isRequired);
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
        }
      });
    }

    if (schema.type === "object" && !schema.properties && schema.additionalProperties === true) {
      return z.any().superRefine((value, ctx) => {
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

const getAllFieldPaths = (values: FieldValues, prefix = ""): string[] => {
  const paths: string[] = [];

  for (const key in values) {
    const value = values[key];
    const path = prefix ? `${prefix}.${key}` : key;
    paths.push(path);

    if (Array.isArray(value)) {
      value.forEach((item, index) => {
        const itemPath = `${path}.${index}`;
        if (typeof item === "object" && item !== null) {
          paths.push(itemPath);
          paths.push(...getAllFieldPaths(item, itemPath));
        }
      });
    } else if (typeof value === "object" && value !== null) {
      paths.push(...getAllFieldPaths(value, path));
    }
  }

  return paths;
};
