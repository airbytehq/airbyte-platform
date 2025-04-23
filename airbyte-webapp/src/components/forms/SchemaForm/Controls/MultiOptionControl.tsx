import classNames from "classnames";
import isBoolean from "lodash/isBoolean";
import { useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";

import { ListBox } from "components/ui/ListBox";

import { AdditionalPropertiesControl } from "./AdditionalPropertiesControl";
import { ControlGroup } from "./ControlGroup";
import styles from "./MultiOptionControl.module.scss";
import { ObjectControl } from "./ObjectControl";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps, OverrideByPath, BaseControlProps } from "./types";
import { useToggleConfig } from "./useToggleConfig";
import { useSchemaForm } from "../SchemaForm";
import { AirbyteJsonSchema, extractDefaultValuesFromSchema, getSelectedOptionSchema } from "../utils";

export const MultiOptionControl = ({
  fieldSchema,
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  hideBorder = false,
}: BaseControlComponentProps) => {
  const value: unknown = useWatch({ name: baseProps.name });
  const { setValue, clearErrors } = useFormContext();
  const { errorAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name, fieldSchema);
  const error = errorAtPath(baseProps.name);
  const optionSchemas = fieldSchema.oneOf ?? fieldSchema.anyOf;
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

  if (options.length === 1) {
    return (
      <ObjectControl
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        fieldSchema={options[0]}
      />
    );
  }

  if (!optionSchemas) {
    return null;
  }

  if (hideBorder) {
    return <>{renderOptionContents(baseProps, selectedOption, overrideByPath, skipRenderedPathRegistration)}</>;
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

  if (selectedOption.properties) {
    return (
      <ObjectControl
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        hideBorder
        fieldSchema={selectedOption}
      />
    );
  }

  if (selectedOption.additionalProperties && !isBoolean(selectedOption.additionalProperties)) {
    return (
      <AdditionalPropertiesControl
        baseProps={baseProps}
        fieldSchema={selectedOption.additionalProperties}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        hideBorder
      />
    );
  }

  return (
    <SchemaFormControl
      key={baseProps.name}
      path={baseProps.name}
      overrideByPath={overrideByPath}
      skipRenderedPathRegistration={skipRenderedPathRegistration}
      fieldSchema={{
        ...selectedOption,
        // If the selectedOption has no title, then don't render any title for this field
        // since the parent parent MultiOptionControl is already rendering the title
        title: selectedOption.title ?? "",
      }}
      isRequired
    />
  );
};
