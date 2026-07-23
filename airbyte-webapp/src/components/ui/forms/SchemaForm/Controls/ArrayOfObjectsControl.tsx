/* eslint-disable @typescript-eslint/no-explicit-any */
import classNames from "classnames";
import isBoolean from "lodash/isBoolean";
import { useCallback, useMemo } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import styles from "./ArrayOfObjectsControl.module.scss";
import { ControlGroup } from "./ControlGroup";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps } from "./types";
import { useSchemaForm } from "../SchemaForm";
import { useErrorAtPath } from "../useErrorAtPath";
import { AirbyteJsonSchema } from "../utils";

export const ArrayOfObjectsControl = ({
  fieldSchema,
  baseProps,
  skipRenderedPathRegistration = false,
  overrideByPath = {},
}: BaseControlComponentProps) => {
  const { extractDefaultValuesFromSchema } = useSchemaForm();
  const { setValue } = useFormContext();
  const value: unknown[] | undefined = useWatch({ name: baseProps.name });
  const items = useMemo(() => value ?? [], [value]);

  const append = useCallback(
    (item: unknown) => {
      setValue(baseProps.name, [...items, item]);
    },
    [items, setValue, baseProps.name]
  );

  const remove = useCallback(
    (index: number) => {
      setValue(
        baseProps.name,
        items.filter((_, i) => i !== index)
      );
    },
    [items, setValue, baseProps.name]
  );

  // react-hook-form adds "root" to the path of errors on the array of objects field
  const error = useErrorAtPath(`${baseProps.name}.root`);

  if (!fieldSchema.items) {
    throw new Error("items is required on array of object fields");
  }
  if (isBoolean(fieldSchema.items) || Array.isArray(fieldSchema.items)) {
    throw new Error("items must be an object");
  }
  const itemSchema = fieldSchema.items as AirbyteJsonSchema;
  const itemDefaultValues = extractDefaultValuesFromSchema(itemSchema);

  return (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      optional={baseProps.optional}
      path={baseProps.name}
      error={error}
      header={baseProps.header}
      data-field-path={baseProps["data-field-path"]}
      disabled={baseProps.disabled}
    >
      {items.map((_, index) => (
        <FlexContainer key={index} alignItems="flex-start">
          <SchemaFormControl
            className={styles.itemControl}
            path={`${baseProps.name}.${index}`}
            overrideByPath={overrideByPath}
            skipRenderedPathRegistration={skipRenderedPathRegistration}
            fieldSchema={itemSchema}
            isRequired
          />
          <RemoveButton
            className={classNames({ [styles.removeButtonPadding]: hasGroupedItems(itemSchema) })}
            onClick={() => remove(index)}
            disabled={baseProps.disabled}
          />
        </FlexContainer>
      ))}
      <div className={styles.addButtonContainer}>
        <Button
          variant="secondary"
          onClick={() => append(itemDefaultValues)}
          type="button"
          icon="plus"
          data-testid={`add-item-_${baseProps.name}`}
          disabled={baseProps.disabled}
        >
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

const hasGroupedItems = (schema: AirbyteJsonSchema): boolean => {
  if (schema.type === "object" || schema.oneOf || schema.anyOf) {
    return true;
  }
  if (schema.type === "array" && schema.items && !isBoolean(schema.items) && !Array.isArray(schema.items)) {
    const itemSchema = schema.items as AirbyteJsonSchema;
    if (itemSchema.type === "object" || itemSchema.type === "array" || itemSchema.oneOf || itemSchema.anyOf) {
      return true;
    }
  }
  return false;
};
