import uniq from "lodash/uniq";
import React from "react";
import { FieldError, UseFormGetFieldState, useController, useFormContext } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { LabeledSwitch } from "components";
import { FlexContainer } from "components/ui/Flex";
import { StatusIcon } from "components/ui/StatusIcon";
import { Text } from "components/ui/Text";

import { FormBaseItem, FORM_PATTERN_ERROR } from "core/form/types";
import { useExperiment } from "hooks/services/Experiment";

import styles from "./PropertySection.module.scss";
import { ConnectorFormValues } from "../../types";
import { useSshSslImprovements } from "../../useSshSslImprovements";
import { getPatternDescriptor, isLocalhost } from "../../utils";
import { Control } from "../Property/Control";
import { PropertyError } from "../Property/PropertyError";
import { PropertyLabel } from "../Property/PropertyLabel";

interface PropertySectionProps {
  property: FormBaseItem;
  path?: string;
  disabled?: boolean;
}

const ErrorMessage = ({ error, property }: { error?: string; property: FormBaseItem }) => {
  const showSimplifiedConfiguration = useExperiment("connector.form.simplifyConfiguration", false);

  if (!error) {
    return null;
  }
  return (
    <PropertyError>
      <FormattedMessage
        id={error}
        values={
          error === FORM_PATTERN_ERROR
            ? {
                pattern: showSimplifiedConfiguration
                  ? getPatternDescriptor(property) ?? property.pattern
                  : property.pattern,
              }
            : undefined
        }
      />
    </PropertyError>
  );
};

const FormatBlock = ({
  property,
  fieldMeta,
  value,
}: {
  property: FormBaseItem;
  fieldMeta: ReturnType<UseFormGetFieldState<ConnectorFormValues>>;
  value: unknown;
}) => {
  const showSimplifiedConfiguration = useExperiment("connector.form.simplifyConfiguration", false);
  if (!showSimplifiedConfiguration) {
    return null;
  }

  const patternDescriptor = getPatternDescriptor(property);
  if (patternDescriptor === undefined) {
    return null;
  }

  const hasPatternError = isPatternError(fieldMeta.error);

  const patternStatus =
    value !== undefined && hasPatternError && fieldMeta.isTouched
      ? "error"
      : value !== undefined && !hasPatternError && property.pattern !== undefined
      ? "success"
      : "none";

  return (
    <FlexContainer alignItems="center" gap="sm">
      {patternStatus !== "none" && <StatusIcon status={patternStatus} size="sm" />}
      <Text className={styles.formatText} size="xs">
        {patternDescriptor}
      </Text>
    </FlexContainer>
  );
};

export const PropertySection: React.FC<PropertySectionProps> = ({ property, path, disabled }) => {
  const { control, getFieldState, watch, formState } = useFormContext();
  const propertyPath = path ?? property.path;
  const { showSshSslImprovements } = useSshSslImprovements();
  const { field } = useController({
    name: propertyPath,
    control,
    rules: {
      validate:
        showSshSslImprovements && propertyPath === "connectionConfiguration.tunnel_method.tunnel_host"
          ? (value: string | undefined) => (isLocalhost(value) ? "form.noLocalhost" : undefined)
          : undefined,
    },
  });
  const value = watch(propertyPath);
  const meta = getFieldState(propertyPath, formState);

  const labelText = property.title || property.fieldKey;

  if (property.type === "boolean") {
    const switchId = `switch-${propertyPath}`;
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

  const hasError = isPatternError(meta.error) ? meta.isTouched : !!meta.error;

  const errorMessage = Array.isArray(meta.error) ? (
    <FlexContainer direction="column" gap="none">
      {uniq(meta.error.map((error) => error?.message).filter(Boolean)).map((errorMessage, index) => {
        return <ErrorMessage key={index} error={errorMessage} property={property} />;
      })}
    </FlexContainer>
  ) : (
    <ErrorMessage error={meta.error?.message} property={property} />
  );

  return (
    <PropertyLabel
      className={styles.defaultLabel}
      property={property}
      label={labelText}
      format={<FormatBlock property={property} fieldMeta={meta} value={value} />}
    >
      <Control property={property} name={propertyPath} disabled={disabled} error={hasError} />
      {hasError && errorMessage}
    </PropertyLabel>
  );
};

const isPatternError = (error: FieldError | undefined) => {
  return (Array.isArray(error) ? error : [error]).some((error) => error?.message === FORM_PATTERN_ERROR);
};
