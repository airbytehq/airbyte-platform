import isBoolean from "lodash/isBoolean";
import { useFieldArray } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";

import styles from "./ArrayOfObjectsControl.module.scss";
import { ControlGroup } from "./ControlGroup";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps } from "./types";
import { useSchemaForm } from "../SchemaForm";
import { AirbyteJsonSchema, extractDefaultValuesFromSchema } from "../utils";

export const ArrayOfObjectsControl = ({
  fieldSchema,
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
}: BaseControlComponentProps) => {
  const { errorAtPath } = useSchemaForm();
  const { fields: items, append, remove } = useFieldArray({ name: baseProps.name });
  const error = errorAtPath(baseProps.name);

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
            fieldSchema={itemSchema}
            isRequired
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
