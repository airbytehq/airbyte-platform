import { useField } from "formik";
import uniq from "lodash/uniq";
import React from "react";
import { FormattedMessage } from "react-intl";

import { LabeledSwitch } from "components";
import { FlexContainer } from "components/ui/Flex";

import { FormBaseItem } from "core/form/types";

import styles from "./PropertySection.module.scss";
import { Control } from "../Property/Control";
import { PropertyError } from "../Property/PropertyError";
import { PropertyLabel } from "../Property/PropertyLabel";

interface PropertySectionProps {
  property: FormBaseItem;
  path?: string;
  disabled?: boolean;
}

const ErrorMessage = ({ error, property }: { error?: string; property: FormBaseItem }) => {
  if (!error) {
    return null;
  }
  return (
    <PropertyError>
      <FormattedMessage
        id={error}
        values={error === "form.pattern.error" ? { pattern: property.pattern } : undefined}
      />
    </PropertyError>
  );
};

export const PropertySection: React.FC<PropertySectionProps> = ({ property, path, disabled }) => {
  const propertyPath = path ?? property.path;
  const formikBag = useField(propertyPath);
  const [field, meta] = formikBag;

  const labelText = property.title || property.fieldKey;

  if (property.type === "boolean") {
    const switchId = `switch-${field.name}`;
    return (
      <LabeledSwitch
        {...field}
        id={switchId}
        label={
          <PropertyLabel
            className={styles.switchLabel}
            property={property}
            label={labelText}
            optional={false}
            htmlFor={switchId}
          />
        }
        value={field.value ?? property.default}
        disabled={disabled}
      />
    );
  }

  const hasError = !!meta.error && meta.touched;

  const errorMessage = Array.isArray(meta.error) ? (
    <FlexContainer direction="column" gap="none">
      {uniq(meta.error.filter(Boolean)).map((error, index) => {
        return <ErrorMessage key={index} error={error} property={property} />;
      })}
    </FlexContainer>
  ) : (
    <ErrorMessage error={meta.error} property={property} />
  );

  return (
    <PropertyLabel className={styles.defaultLabel} property={property} label={labelText}>
      <Control property={property} name={propertyPath} disabled={disabled} error={hasError} />
      {hasError && errorMessage}
    </PropertyLabel>
  );
};
