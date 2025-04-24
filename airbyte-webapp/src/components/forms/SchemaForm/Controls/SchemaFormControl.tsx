import { useFormContext, useWatch } from "react-hook-form";
import { useUnmount } from "react-use";

import { LabelInfo } from "components/Label";

import { ArrayOfObjectsControl } from "./ArrayOfObjectsControl";
import { MultiOptionControl } from "./MultiOptionControl";
import { ObjectControl } from "./ObjectControl";
import { OverrideByPath, BaseControlProps } from "./types";
import { FormControl } from "../../FormControl";
import { LinkComponentsToggle } from "../LinkComponentsToggle";
import { useSchemaForm } from "../SchemaForm";
import { AirbyteJsonSchema, displayName, nestPath, resolveTopLevelRef } from "../utils";

interface SchemaFormControlProps {
  /**
   * Path to the property in the schema. Empty string or undefined for root level properties.
   */
  path?: string;

  /**
   * Map of property paths to custom renderers, allowing override of specific fields.
   */
  overrideByPath?: OverrideByPath;

  /**
   * If true, the component will not register the path as rendered.
   * Used internally by SchemaFormRemainingFields to prevent duplicate registration.
   */
  skipRenderedPathRegistration?: boolean;

  /**
   * The schema for the field currently being rendered.
   * If not provided, the schema will be resolved from the root schema and the path.
   */
  fieldSchema?: AirbyteJsonSchema;

  /**
   * If provided, these fields will be rendered normally, and everything else will
   * be rendered inside a collapsible Advanced section.
   *
   * If not provided, all fields will be rendered normally.
   */
  nonAdvancedFields?: string[];

  titleOverride?: string;
  isRequired?: boolean;
  className?: string;
}

/**
 * Component that renders form controls based on JSON schema.
 * It can render a single field or recursively render nested objects.
 */
export const SchemaFormControl = ({
  path = "",
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  fieldSchema,
  titleOverride,
  isRequired = true,
  className,
  nonAdvancedFields,
}: SchemaFormControlProps) => {
  const {
    schema: rootSchema,
    getSchemaAtPath,
    registerRenderedPath,
    nestedUnderPath,
    verifyArrayItems,
    convertJsonSchemaToZodSchema,
  } = useSchemaForm();
  const { register, clearErrors } = useFormContext();

  const targetPath = path ? path : nestPath(path, nestedUnderPath);

  const value = useWatch({ name: targetPath });

  // Register this path synchronously during render
  if (!skipRenderedPathRegistration && path) {
    registerRenderedPath(path);
  }

  // Get the property at the specified path
  const targetSchema = resolveTopLevelRef(rootSchema, fieldSchema ?? getSchemaAtPath(path, value));

  useUnmount(() => {
    // Remove the validation logic for this field when unmounting.
    // Using unregister() fully removes the field from the form which causes other issues,
    // so just use register() to replace the validation logic with a no-op.
    register(targetPath, {
      validate: () => {
        return true;
      },
    });
    clearErrors(targetPath);
  });

  // Register validation logic for this field
  register(targetPath, {
    validate: (value) => {
      const zodSchema = convertJsonSchemaToZodSchema(targetSchema, isRequired);
      const result = zodSchema.safeParse(value);
      if (result.success === false) {
        return result.error.issues.at(-1)?.message;
      }
      return true;
    },
  });

  // ~ declarative_component_schema type $parameters handling ~
  if (path.includes("$parameters")) {
    return null;
  }

  // Check if there's an override for this path
  if (overrideByPath[path] !== undefined) {
    return overrideByPath[path];
  }

  if (targetSchema.deprecated) {
    return null;
  }

  const baseProps: BaseControlProps = {
    name: targetPath,
    label: titleOverride ?? displayName(path, targetSchema.title),
    labelTooltip:
      targetSchema.description || targetSchema.examples ? (
        <LabelInfo description={targetSchema.description} examples={targetSchema.examples} />
      ) : undefined,
    optional: !isRequired,
    header: <LinkComponentsToggle path={path} fieldSchema={targetSchema} />,
    containerControlClassName: className,
    onlyShowErrorIfTouched: true,
  };

  if (targetSchema.oneOf || targetSchema.anyOf) {
    return (
      <MultiOptionControl
        fieldSchema={targetSchema}
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        nonAdvancedFields={nonAdvancedFields}
      />
    );
  }

  if (targetSchema.type === "object") {
    return (
      <ObjectControl
        fieldSchema={targetSchema}
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        nonAdvancedFields={nonAdvancedFields}
      />
    );
  }

  if (targetSchema.type === "boolean") {
    return <FormControl {...baseProps} fieldType="switch" />;
  }

  if (targetSchema.type === "string") {
    if (targetSchema.enum) {
      const options = Array.isArray(targetSchema.enum)
        ? targetSchema.enum.map((option: string) => ({
            label: option,
            value: option,
          }))
        : [];

      return <FormControl {...baseProps} fieldType="dropdown" options={options} />;
    }
    if (targetSchema.multiline) {
      return <FormControl {...baseProps} fieldType="textarea" />;
    }
    return <FormControl {...baseProps} fieldType="input" />;
  }

  if (targetSchema.type === "number" || targetSchema.type === "integer") {
    return <FormControl {...baseProps} fieldType="input" type="number" />;
  }

  if (targetSchema.type === "array") {
    const items = verifyArrayItems(targetSchema.items);
    if (items.type === "object" || items.type === "array") {
      return (
        <ArrayOfObjectsControl
          fieldSchema={targetSchema}
          baseProps={baseProps}
          overrideByPath={overrideByPath}
          skipRenderedPathRegistration={skipRenderedPathRegistration}
        />
      );
    }
    if (items.type === "string" || items.type === "integer" || items.type === "number") {
      return <FormControl {...baseProps} fieldType="array" itemType={items.type} />;
    }
  }

  return null;
};
