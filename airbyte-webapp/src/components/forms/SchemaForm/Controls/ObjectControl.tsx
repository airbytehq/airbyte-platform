import isBoolean from "lodash/isBoolean";
import { get, useFormState } from "react-hook-form";

import { Collapsible } from "components/ui/Collapsible";

import { AdditionalPropertiesControl } from "./AdditionalPropertiesControl";
import { ControlGroup } from "./ControlGroup";
import styles from "./ObjectControl.module.scss";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps } from "./types";
import { useToggleConfig } from "./useToggleConfig";
import { useSchemaForm } from "../SchemaForm";
import { AirbyteJsonSchema, getDeclarativeSchemaTypeValue } from "../utils";

export const ObjectControl = ({
  fieldSchema,
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  hideBorder = false,
  nonAdvancedFields,
}: BaseControlComponentProps) => {
  const { errorAtPath } = useSchemaForm();
  const { errors } = useFormState();
  const toggleConfig = useToggleConfig(baseProps.name, fieldSchema);

  if (!fieldSchema.properties) {
    if (!fieldSchema.additionalProperties) {
      return null;
    }
    const additionalPropertiesSchema = fieldSchema.additionalProperties;
    if (isBoolean(additionalPropertiesSchema)) {
      return null;
    }

    return (
      <AdditionalPropertiesControl
        fieldSchema={additionalPropertiesSchema}
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

  const nonAdvancedElements: JSX.Element[] = [];
  const advancedElements: JSX.Element[] = [];
  let hasErrorInAdvanced = false;

  Object.entries(fieldSchema.properties).forEach(([propertyName, property]) => {
    const isAdvanced = !nonAdvancedFields
      ? false
      : !nonAdvancedFields.some((field) => field === propertyName || field.startsWith(`${propertyName}.`));

    const nonAdvancedSubfields = isAdvanced
      ? []
      : !nonAdvancedFields
      ? []
      : nonAdvancedFields
          .filter((field) => field.startsWith(`${propertyName}.`))
          .map((field) => field.slice(propertyName.length + 1));

    const fullPath = baseProps.name ? `${baseProps.name}.${propertyName}` : propertyName;

    // ~ declarative_component_schema type handling ~
    if (getDeclarativeSchemaTypeValue(propertyName, property)) {
      return;
    }

    const element = (
      <SchemaFormControl
        key={fullPath}
        path={fullPath}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
        fieldSchema={property as AirbyteJsonSchema}
        isRequired={fieldSchema.required?.includes(propertyName) ?? false}
        nonAdvancedFields={nonAdvancedSubfields.length > 0 ? nonAdvancedSubfields : undefined}
      />
    );

    if (isAdvanced) {
      advancedElements.push(element);
      if (get(errors, fullPath)) {
        hasErrorInAdvanced = true;
      }
    } else {
      nonAdvancedElements.push(element);
    }
  });

  const contents = (
    <>
      {nonAdvancedElements.length > 0 && nonAdvancedElements}
      {advancedElements.length > 0 && (
        <Collapsible className={styles.advancedCollapsible} label="Advanced" showErrorIndicator={hasErrorInAdvanced}>
          {advancedElements}
        </Collapsible>
      )}
    </>
  );

  if (!baseProps.name || hideBorder) {
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
