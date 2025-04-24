import isBoolean from "lodash/isBoolean";
import { useEffect, useState } from "react";
import { get, useFormState, useFormContext, useWatch } from "react-hook-form";
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
import { AirbyteJsonSchema, getDeclarativeSchemaTypeValue, displayName } from "../utils";
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

const JsonEditor = ({
  baseProps,
  toggleConfig,
}: {
  baseProps: BaseControlProps;
  toggleConfig: ReturnType<typeof useToggleConfig>;
}) => {
  const { formatMessage } = useIntl();
  const { errorAtPath } = useSchemaForm();
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
      error={errorAtPath(baseProps.name)}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
      footer={!textValue ? formatMessage({ id: "form.enterValidJson" }) : undefined}
    >
      <FlexContainer className={styles.jsonEditorContainer} direction="column" gap="md">
        <div className={styles.jsonEditor}>
          <CodeEditor
            key={baseProps.name}
            value={textValue}
            language="json"
            onChange={(val: string | undefined) => {
              setTextValue(val || "");
              let parsedValue = val;
              try {
                parsedValue = JSON.parse(val || "{}");
              } catch (error) {}
              setValue(baseProps.name, parsedValue, { shouldValidate: true, shouldTouch: true });
            }}
          />
        </div>
      </FlexContainer>
    </ControlGroup>
  );
};
