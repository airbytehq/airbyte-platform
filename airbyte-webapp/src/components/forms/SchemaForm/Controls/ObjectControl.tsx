import isBoolean from "lodash/isBoolean";
import { useEffect, useState } from "react";
import { get, useFormContext, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";

import { CodeEditor } from "components/ui/CodeEditor";
import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";

import { AdditionalPropertiesControl } from "./AdditionalPropertiesControl";
import { ControlGroup } from "./ControlGroup";
import styles from "./ObjectControl.module.scss";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps, BaseControlProps } from "./types";
import { useToggleConfig } from "./useToggleConfig";
import { useSchemaForm } from "../SchemaForm";
import { useErrorAtPath } from "../useErrorAtPath";
import { AirbyteJsonSchema, getDeclarativeSchemaTypeValue, displayName } from "../utils";

export const ObjectControl = ({
  fieldSchema,
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  hideBorder = false,
  nonAdvancedFields,
}: BaseControlComponentProps) => {
  const { overrideByObjectField } = useSchemaForm();
  const value = useWatch({ name: baseProps.name });
  const toggleConfig = useToggleConfig(baseProps.name, fieldSchema);
  const error = useErrorAtPath(baseProps.name);

  if (!fieldSchema.properties) {
    if (!fieldSchema.additionalProperties) {
      return null;
    }

    if (isBoolean(fieldSchema.additionalProperties)) {
      return <JsonEditor baseProps={baseProps} toggleConfig={toggleConfig} />;
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
  let hasAdvancedValue = false;

  const declarativeSchemaType = getDeclarativeSchemaTypeValue("type", fieldSchema.properties.type);

  Object.entries(fieldSchema.properties)
    .filter(([propertyName]) => propertyName !== "$parameters")
    .forEach(([propertyName, property]) => {
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

      const element = overrideByObjectField?.[declarativeSchemaType]?.fieldOverrides?.[propertyName]?.(fullPath) ?? (
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
        const advancedPropertyValue = get(value, propertyName);
        if (advancedPropertyValue && !isBoolean(property) && advancedPropertyValue !== property.default) {
          hasAdvancedValue = true;
        }
      } else {
        nonAdvancedElements.push(element);
      }
    });

  const contents = (
    <>
      {nonAdvancedElements.length > 0 && nonAdvancedElements}
      {advancedElements.length > 0 && (
        <Collapsible className={styles.advancedCollapsible} label="Advanced" initiallyOpen={hasAdvancedValue}>
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
      error={error}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
      header={baseProps.header}
      data-field-path={baseProps["data-field-path"]}
      disabled={baseProps.disabled}
    >
      {contents}
    </ControlGroup>
  );
};

const JsonEditor = ({
  baseProps,
  toggleConfig,
}: {
  baseProps: BaseControlProps;
  toggleConfig: ReturnType<typeof useToggleConfig>;
}) => {
  const { formatMessage } = useIntl();
  const error = useErrorAtPath(baseProps.name);
  const value = useWatch({ name: baseProps.name });
  const { setValue } = useFormContext();
  const [textValue, setTextValue] = useState(JSON.stringify(value, null, 2));
  useEffect(() => {
    // reset the textValue state when this is toggled off
    if (value === undefined) {
      setTextValue("");
    }
  }, [value]);

  return (
    <ControlGroup
      path={baseProps.name}
      title={displayName(baseProps.name, baseProps.label)}
      tooltip={baseProps.labelTooltip}
      optional={baseProps.optional}
      error={error}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
      footer={!textValue ? formatMessage({ id: "form.enterValidJson" }) : undefined}
      data-field-path={baseProps["data-field-path"]}
      disabled={baseProps.disabled}
    >
      <FlexContainer className={styles.jsonEditorContainer} direction="column" gap="md">
        <div className={styles.jsonEditor}>
          <CodeEditor
            key={baseProps.name}
            value={textValue}
            language="json"
            readOnly={baseProps.disabled}
            onChange={(val: string | undefined) => {
              setTextValue(val || "");
              try {
                const parsedValue = JSON.parse(val || "{}");
                setValue(baseProps.name, parsedValue, { shouldValidate: true, shouldTouch: true });
              } catch (error) {
                setValue(baseProps.name, val, { shouldValidate: true, shouldTouch: true });
              }
            }}
          />
        </div>
      </FlexContainer>
    </ControlGroup>
  );
};
