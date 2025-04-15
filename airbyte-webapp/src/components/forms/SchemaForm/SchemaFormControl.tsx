import isBoolean from "lodash/isBoolean";
import { ReactElement, useCallback } from "react";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { LabelInfo } from "components/Label";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { ListBox } from "components/ui/ListBox";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import { ControlGroup } from "./ControlGroup";
import { useSchemaForm } from "./SchemaForm";
import styles from "./SchemaFormControl.module.scss";
import { extractDefaultValuesFromSchema, verifyArrayItems, AirbyteJsonSchema, getSelectedOptionSchema } from "./utils";
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
}

type OverrideByPath = Record<string, ReactElement | null>;

interface BaseControlProps {
  name: string;
  label?: string;
  labelTooltip?: ReactElement;
  optional: boolean;
}

/**
 * Component that renders form controls based on JSON schema.
 * It can render a single field or recursively render nested objects.
 */
export const SchemaFormControl = ({ path = "", overrideByPath = {} }: SchemaFormControlProps) => {
  const { schemaAtPath, isRequiredField } = useSchemaForm();

  // Check if there's an override for this path
  if (overrideByPath[path] !== undefined) {
    return overrideByPath[path];
  }

  // Get the property at the specified path
  const targetProperty = schemaAtPath(path);

  const baseProps: BaseControlProps = {
    name: path,
    label: targetProperty.title,
    labelTooltip:
      targetProperty.description || targetProperty.examples ? (
        <LabelInfo description={targetProperty.description} examples={targetProperty.examples} />
      ) : undefined,
    optional: !isRequiredField(path),
  };

  if (targetProperty.type === "object") {
    return <ObjectControl baseProps={baseProps} overrideByPath={overrideByPath} />;
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
    const items = verifyArrayItems(targetProperty.items);
    if (items.type === "object") {
      return <ArrayOfObjectsControl baseProps={baseProps} overrideByPath={overrideByPath} />;
    }
    if (items.type === "string") {
      return <FormControl {...baseProps} fieldType="array" />;
    }
  }

  return null;
};

const ObjectControl = ({
  baseProps,
  overrideByPath = {},
}: {
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
}) => {
  const { errorAtPath, schemaAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name);

  const objectProperty = schemaAtPath(baseProps.name);
  if (objectProperty.oneOf || objectProperty.anyOf) {
    return <MultiOptionControl baseProps={baseProps} overrideByPath={overrideByPath} />;
  }

  // If no properties, nothing to render
  if (!objectProperty.properties) {
    return null;
  }

  const contents = (
    <>
      {Object.keys(objectProperty.properties).map((propertyName) => {
        const fullPath = baseProps.name ? `${baseProps.name}.${propertyName}` : propertyName;
        return <SchemaFormControl key={fullPath} path={fullPath} overrideByPath={overrideByPath} />;
      })}
    </>
  );

  // rendering top-level schema, so no border is necessary
  if (!baseProps.name || !baseProps.label) {
    return contents;
  }

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={errorAtPath(baseProps.name)}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
    >
      {contents}
    </ControlGroup>
  );
};

const MultiOptionControl = ({
  baseProps,
  overrideByPath = {},
}: {
  baseProps: BaseControlProps;
  overrideByPath?: OverrideByPath;
}) => {
  const value: unknown = useWatch({ name: baseProps.name });
  const { setValue, clearErrors } = useFormContext();
  const { schemaAtPath, errorAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name);
  const property = schemaAtPath(baseProps.name);
  const optionSchemas = property.oneOf ?? property.anyOf;

  if (!optionSchemas) {
    return null;
  }

  const options = optionSchemas.filter((optionSchema) => !isBoolean(optionSchema)) as AirbyteJsonSchema[];
  const selectedOption = getSelectedOptionSchema(options, value);

  // Render the selected option's properties
  const renderOptionContents = () => {
    if (!selectedOption || !selectedOption.properties) {
      return null;
    }

    return Object.keys(selectedOption.properties).map((propertyName) => {
      // ~ declarative_component_schema type handling ~
      if (propertyName === "type") {
        return null;
      }

      const fullPath = baseProps.name ? `${baseProps.name}.${propertyName}` : propertyName;
      return <SchemaFormControl key={fullPath} path={fullPath} overrideByPath={overrideByPath} />;
    });
  };

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={errorAtPath(baseProps.name)}
      control={
        <ListBox
          options={options.map((option) => ({
            label: option.title,
            value: option.title,
          }))}
          onSelect={(optionTitle) => {
            const selectedOption = options.find((option) => option.title === optionTitle);
            if (!selectedOption) {
              setValue(baseProps.name, undefined);
              return;
            }

            // Set values WITHOUT validation
            // debugger;
            const defaultValues = extractDefaultValuesFromSchema(selectedOption);
            setValue(baseProps.name, defaultValues, { shouldValidate: false });

            // Only clear the error for the parent field itself, without validating
            clearErrors(baseProps.name);
          }}
          selectedValue={selectedOption?.title}
          adaptiveWidth={false}
        />
      }
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
    >
      {renderOptionContents()}
    </ControlGroup>
  );
};

const ArrayOfObjectsControl = ({
  baseProps,
  overrideByPath = {},
}: {
  baseProps: BaseControlProps;
  overrideByPath?: Record<string, ReactElement | null>;
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
      control={
        <Button variant="secondary" onClick={() => append(itemDefaultValues)} type="button">
          <FormattedMessage id="form.add" />
        </Button>
      }
    >
      {items.map((item, index) => (
        <FlexContainer key={item.id} alignItems="flex-start">
          <SchemaFormControl path={`${baseProps.name}.${index}`} overrideByPath={overrideByPath} />
          <RemoveButton className={styles.removeButton} onClick={() => remove(index)} />
        </FlexContainer>
      ))}
    </ControlGroup>
  );
};

const useToggleConfig = (path: string) => {
  const { schemaAtPath } = useSchemaForm();
  const { setValue, clearErrors } = useFormContext();
  const value = useWatch({ name: path });

  const defaultValue = extractDefaultValuesFromSchema(schemaAtPath(path));
  const handleToggle = useCallback(
    (newEnabledState: boolean) => {
      if (newEnabledState) {
        setValue(path, defaultValue);
      } else {
        setValue(path, undefined);
        clearErrors(path);
      }
    },
    [path, clearErrors, defaultValue, setValue]
  );

  return {
    isEnabled: !!value,
    onToggle: handleToggle,
  };
};
