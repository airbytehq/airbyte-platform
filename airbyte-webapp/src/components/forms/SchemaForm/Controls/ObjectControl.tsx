import isBoolean from "lodash/isBoolean";

import { AdditionalPropertiesControl } from "./AdditionalPropertiesControl";
import { ControlGroup } from "./ControlGroup";
import { MultiOptionControl } from "./MultiOptionControl";
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
}: BaseControlComponentProps) => {
  const { errorAtPath } = useSchemaForm();
  const toggleConfig = useToggleConfig(baseProps.name, fieldSchema);

  if (fieldSchema.oneOf || fieldSchema.anyOf) {
    return (
      <MultiOptionControl
        fieldSchema={fieldSchema}
        baseProps={baseProps}
        overrideByPath={overrideByPath}
        skipRenderedPathRegistration={skipRenderedPathRegistration}
      />
    );
  }

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

  const contents = (
    <>
      {Object.entries(fieldSchema.properties).map(([propertyName, property]) => {
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
            fieldSchema={property as AirbyteJsonSchema}
            isRequired={fieldSchema.required?.includes(propertyName) ?? false}
          />
        );
      })}
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
