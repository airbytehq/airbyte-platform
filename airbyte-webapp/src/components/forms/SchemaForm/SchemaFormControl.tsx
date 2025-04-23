import classNames from "classnames";
import isBoolean from "lodash/isBoolean";
import { ReactElement, useCallback, useMemo } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { LabelInfo } from "components/Label";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import { ControlGroup } from "./ControlGroup";
import { LinkComponentsToggle } from "./LinkComponentsToggle";
import { useRefsHandler } from "./RefsHandler";
import { useSchemaForm } from "./SchemaForm";
import styles from "./SchemaFormControl.module.scss";
import {
  extractDefaultValuesFromSchema,
  verifyArrayItems,
  AirbyteJsonSchema,
  getSelectedOptionSchema,
  getDeclarativeSchemaTypeValue,
} from "./utils";
import { FormControl } from "../FormControl";

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
   * If true, the component will extract the multi option schema from the oneOf or anyOf array.
   */
  extractMultiOptionSchema?: boolean;
}

type OverrideByPath = Record<string, ReactElement | null>;

interface BaseControlProps {
  name: string;
  label?: string;
  labelTooltip?: ReactElement;
  optional: boolean;
  header?: ReactElement;
}

/**
 * Component that renders form controls based on JSON schema.
 * It can render a single field or recursively render nested objects.
 */
export const SchemaFormControl = ({
  path = "",
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  extractMultiOptionSchema = false,
}: SchemaFormControlProps) => {
  const { schemaAtPath, isRequiredField, registerRenderedPath } = useSchemaForm();

  // Register this path synchronously during render instead of in an effect
  if (!skipRenderedPathRegistration && path) {
    registerRenderedPath(path);
  }

  // Check if there's an override for this path
  if (overrideByPath[path] !== undefined) {
    return overrideByPath[path];
  }

  // Get the property at the specified path
  const targetProperty = schemaAtPath(path, extractMultiOptionSchema);
  if (targetProperty.deprecated) {
    return null;
  }

  const baseProps: BaseControlProps = {
    name: path,
    label: targetProperty.title,
    labelTooltip:
      targetProperty.description || targetProperty.examples ? (
        <LabelInfo description={targetProperty.description} examples={targetProperty.examples} />
      ) : undefined,
    optional: !isRequiredField(path),
    header: <LinkComponentsToggle path={path} />,
  };

  if (targetProperty.oneOf || targetProperty.anyOf) {
    return (
      <MultiOptionControl
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

  if (targetProperty.type === "object") {
    return (
      <ObjectControl
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

  if (targetProperty.type === "boolean") {
    return <FormControl {...baseProps} fieldType="switch" />;
  }

  if (targetProperty.type === "string") {
    if (targetProperty.enum) {
      const options = Array.isArray(targetProperty.enum)
        ? targetProperty.enum.map((option: string) => ({
            label: option,
            value: option,
          }))
        : [];

      return <FormControl {...baseProps} fieldType="dropdown" options={options} />;
    }
    if (targetProperty.multiline) {
      return <FormControl {...baseProps} fieldType="textarea" />;
    }
    return <FormControl {...baseProps} fieldType="input" />;
  }

  if (targetProperty.type === "number" || targetProperty.type === "integer") {
    return <FormControl {...baseProps} fieldType="input" type="number" />;
  }

  if (targetProperty.type === "array") {
    const items = verifyArrayItems(targetProperty.items, path);
    if (items.type === "object") {
      return (
        <ArrayOfObjectsControl
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

const ObjectControl = ({
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
}: {
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
  skipRenderedPathRegistration?: boolean;
}) => {
  const { errorAtPath, schemaAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name);

  const objectProperty = schemaAtPath(baseProps.name);
  if (objectProperty.oneOf || objectProperty.anyOf) {
    return (
      <MultiOptionControl
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

  // If no properties, nothing to render
  if (!objectProperty.properties) {
    return null;
  }

  const contents = (
    <>
      {Object.entries(objectProperty.properties).map(([propertyName, property]) => {
        const fullPath = baseProps.name ? `${baseProps.name}.${propertyName}` : propertyName;
        // ~ declarative_component_schema type handling ~
        if (getDeclarativeSchemaTypeValue(propertyName, property)) {
          return null;
        }

        return (
          <SchemaFormControl
            key={fullPath}
            path={fullPath}
            overrideByPath={overrideByPath}
            skipRenderedPathRegistration={skipRenderedPathRegistration}
          />
        );
      })}
    </>
  );

  // rendering top-level schema, so no border is necessary
  if (!baseProps.name) {
    return (
      <>
        {baseProps.header}
        {contents}
      </>
    );
  }

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={errorAtPath(baseProps.name)}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
      header={baseProps.header}
    >
      {contents}
    </ControlGroup>
  );
};

const MultiOptionControl = ({
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
}: {
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
  skipRenderedPathRegistration?: boolean;
}) => {
  const value: unknown = useWatch({ name: baseProps.name });
  const { setValue, clearErrors } = useFormContext();
  const { schemaAtPath, errorAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name);
  const property = schemaAtPath(baseProps.name);
  const error = errorAtPath(baseProps.name);
  const optionSchemas = property.oneOf ?? property.anyOf;
  const options = useMemo(
    () =>
      optionSchemas?.filter(
        (optionSchema) => !isBoolean(optionSchema) && !optionSchema.deprecated
      ) as AirbyteJsonSchema[],
    [optionSchemas]
  );
  const selectedOption = useMemo(
    () => (options ? getSelectedOptionSchema(options, value) : undefined),
    [options, value]
  );
  const displayError = useMemo(() => (selectedOption?.type === "object" ? error : undefined), [selectedOption, error]);

  if (!optionSchemas) {
    return null;
  }

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={displayError}
      header={baseProps.header}
      control={
        <ListBox
          className={classNames({ [styles.listBoxError]: !!displayError })}
          options={options.map((option) => ({
            label: option.title ?? option.type,
            value: option.title ?? option.type,
          }))}
          onSelect={(selectedValue) => {
            const selectedOption = options.find((option) =>
              option.title ? option.title === selectedValue : option.type === selectedValue
            );
            if (!selectedOption) {
              setValue(baseProps.name, undefined);
              return;
            }

            const defaultValues = extractDefaultValuesFromSchema(selectedOption);
            setValue(baseProps.name, defaultValues, { shouldValidate: false });

            // Only clear the error for the parent field itself, without validating
            clearErrors(baseProps.name);
          }}
          selectedValue={selectedOption?.title ?? selectedOption?.type}
          adaptiveWidth={false}
        />
      }
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
    >
      {renderOptionContents(baseProps, selectedOption, overrideByPath, skipRenderedPathRegistration)}
    </ControlGroup>
  );
};

// Render the selected option's properties
const renderOptionContents = (
  baseProps: BaseControlProps,
  selectedOption?: AirbyteJsonSchema,
  overrideByPath?: OverrideByPath,
  skipRenderedPathRegistration?: boolean
) => {
  if (!selectedOption) {
    return null;
  }

  if (selectedOption.type !== "object") {
    return (
      <SchemaFormControl
        key={baseProps.name}
        path={baseProps.name}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        extractMultiOptionSchema
      />
    );
  }

  // TODO: handle objects with only additionalProperties
  if (!selectedOption.properties) {
    return null;
  }

  return Object.entries(selectedOption.properties).map(([propertyName, property]) => {
    // ~ declarative_component_schema type handling ~
    if (getDeclarativeSchemaTypeValue(propertyName, property)) {
      return null;
    }

    const fullPath = baseProps.name ? `${baseProps.name}.${propertyName}` : propertyName;
    return (
      <SchemaFormControl
        key={fullPath}
        path={fullPath}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  });
};

const ArrayOfObjectsControl = ({
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
}: {
  baseProps: BaseControlProps;
  overrideByPath?: Record<string, ReactElement | null>;
  skipRenderedPathRegistration?: boolean;
}) => {
  const { schemaAtPath, errorAtPath } = useSchemaForm();
  const { fields: items, append, remove } = useFieldArray({ name: baseProps.name });
  const error = errorAtPath(baseProps.name);

  const itemSchema = schemaAtPath(`${baseProps.name}.0`);
  const itemDefaultValues = extractDefaultValuesFromSchema(itemSchema);

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={error}
      header={baseProps.header}
    >
      {items.map((item, index) => (
        <FlexContainer key={item.id} alignItems="flex-start">
          <SchemaFormControl
            path={`${baseProps.name}.${index}`}
            overrideByPath={overrideByPath}
            skipRenderedPathRegistration={skipRenderedPathRegistration}
          />
          <RemoveButton className={styles.removeButton} onClick={() => remove(index)} />
        </FlexContainer>
      ))}
      <div className={styles.addButtonContainer}>
        <Button variant="secondary" onClick={() => append(itemDefaultValues)} type="button" icon="plus">
          {itemSchema.title ? (
            <FormattedMessage id="form.addItem" values={{ itemName: itemSchema.title }} />
          ) : (
            <FormattedMessage id="form.add" />
          )}
        </Button>
      </div>
    </ControlGroup>
  );
};

const useToggleConfig = (path: string) => {
  const { schemaAtPath } = useSchemaForm();
  const { setValue, clearErrors, resetField } = useFormContext();
  const value = useWatch({ name: path });
  const { getReferenceInfo, handleUnlinkAction } = useRefsHandler();

  const handleToggle = useCallback(
    (newEnabledState: boolean) => {
      const schema = schemaAtPath(path);
      const defaultValue = extractDefaultValuesFromSchema(schema);
      if (newEnabledState) {
        // Use resetField to ensure the field and all its children are properly reset
        // This avoids the UI showing stale values that aren't in the form state
        resetField(path, { defaultValue });
        setValue(path, defaultValue);
      } else {
        // Get reference info before making changes
        const refInfo = getReferenceInfo(path);

        // For more deterministic behavior, use a more controlled approach:
        // 1. First unlink all affected references if this field has any
        if (refInfo.type !== "none") {
          // Unlink the field
          handleUnlinkAction(path);

          // Give a small delay to ensure React state updates happen in the correct order
          setTimeout(() => {
            // 2. Then uncheck the field
            setValue(path, undefined);
            clearErrors(path);
          }, 0);
        } else {
          // No references, just uncheck the field immediately
          setValue(path, undefined);
          clearErrors(path);
        }
      }
    },
    [schemaAtPath, path, resetField, setValue, getReferenceInfo, handleUnlinkAction, clearErrors]
  );

  return {
    isEnabled: value !== undefined,
    onToggle: handleToggle,
  };
};
