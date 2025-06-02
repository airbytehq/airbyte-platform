import { useMemo } from "react";
import { useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";
import { ReactMarkdown } from "react-markdown/lib/react-markdown";

import { LabelInfo } from "components/Label";
import { Badge } from "components/ui/Badge";
import { Tooltip } from "components/ui/Tooltip";

import { ArrayOfObjectsControl } from "./ArrayOfObjectsControl";
import { MultiOptionControl } from "./MultiOptionControl";
import { ObjectControl } from "./ObjectControl";
import styles from "./SchemaFormControl.module.scss";
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

  titleOverride?: string | null;
  isRequired?: boolean;
  className?: string;
  placeholder?: string;
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
  isRequired,
  className,
  nonAdvancedFields,
  placeholder,
}: SchemaFormControlProps) => {
  const {
    schema: rootSchema,
    getSchemaAtPath,
    registerRenderedPath,
    onlyShowErrorIfTouched,
    nestedUnderPath,
    verifyArrayItems,
    isRequired: isPathRequired,
    disableFormControls,
  } = useSchemaForm();

  const targetPath = useMemo(() => nestPath(path, nestedUnderPath), [nestedUnderPath, path]);

  // Register this path synchronously during render
  if (!skipRenderedPathRegistration && path) {
    registerRenderedPath(path);
  }

  // Get the property at the specified path
  const targetSchema = resolveTopLevelRef(rootSchema, fieldSchema ?? getSchemaAtPath(path));
  const isOptional = useMemo(() => {
    if (isRequired !== undefined) {
      return !isRequired;
    }

    if (!path || targetPath === nestedUnderPath) {
      return false;
    }

    return !isPathRequired(path);
  }, [isPathRequired, isRequired, nestedUnderPath, path, targetPath]);

  const value = useWatch({ name: targetPath });

  // ~ declarative_component_schema type $parameters handling ~
  if (path.includes("$parameters")) {
    return null;
  }

  // Check if there's an override for this path
  if (overrideByPath[path] !== undefined) {
    return overrideByPath[path];
  }

  if (targetSchema.deprecated && value === undefined) {
    return null;
  }

  const baseProps: BaseControlProps = {
    name: targetPath,
    label: titleOverride ? titleOverride : titleOverride === null ? undefined : displayName(path, targetSchema.title),
    labelTooltip:
      targetSchema.description || targetSchema.examples ? (
        <LabelInfo description={targetSchema.description} examples={targetSchema.examples} />
      ) : undefined,
    optional: isOptional,
    header: targetSchema.deprecated ? (
      <DeprecatedBadge message={targetSchema.deprecation_message} />
    ) : (
      <LinkComponentsToggle path={path} fieldSchema={targetSchema} />
    ),
    containerControlClassName: className,
    onlyShowErrorIfTouched,
    placeholder,
    "data-field-path": path,
    disabled: disableFormControls,
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

  if (targetSchema.enum && (targetSchema.type === undefined || targetSchema.type === "string")) {
    const options = Array.isArray(targetSchema.enum)
      ? targetSchema.enum.map((option: string) => ({
          label: option,
          value: option,
        }))
      : [];

    return <FormControl {...baseProps} fieldType="dropdown" options={options} />;
  }

  if (targetSchema.type === "string") {
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
    if (items.type === "string" || items.type === "integer" || items.type === "number") {
      return <FormControl {...baseProps} fieldType="array" itemType={items.type} />;
    }
    return (
      <ArrayOfObjectsControl
        fieldSchema={targetSchema}
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

  return null;
};

const DeprecatedBadge = ({ message }: { message?: string }) => {
  return message ? (
    <Tooltip control={<Badge variant="grey">Deprecated</Badge>} placement="top">
      <ReactMarkdown className={styles.deprecatedTooltip}>{message}</ReactMarkdown>
    </Tooltip>
  ) : (
    <Badge variant="grey">
      <FormattedMessage id="form.deprecated" />
    </Badge>
  );
};
