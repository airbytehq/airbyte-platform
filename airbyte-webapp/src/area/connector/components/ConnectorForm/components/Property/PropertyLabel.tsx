import React, { useMemo } from "react";

import { LabelInfo } from "components/ui/Label";
import { ControlLabels, ControlLabelsProps } from "components/ui/LabeledControl";

import { FormBlock } from "core/form/types";

interface PropertyLabelProps {
  property: FormBlock;
  label: React.ReactNode;
  description?: string;
  hideOptionalLabel?: boolean;
  className?: string;
  htmlFor?: string;
  format?: React.ReactNode;
}

const getRequirement = (isRequired: boolean, hideOptionalLabel?: boolean): ControlLabelsProps["requirement"] => {
  if (isRequired) {
    return "required";
  }
  if (hideOptionalLabel) {
    return undefined;
  }
  return "optional";
};

export const PropertyLabel: React.FC<React.PropsWithChildren<PropertyLabelProps>> = ({
  property,
  label,
  description,
  hideOptionalLabel,
  className,
  children,
  htmlFor,
  format,
}) => {
  const examples = property._type === "formItem" || property._type === "formGroup" ? property.examples : undefined;
  const descriptionToDisplay = description ?? property.description;

  const optionDescriptions = useMemo(() => {
    if (property._type !== "formCondition") {
      return;
    }
    return Object.entries(property.conditions).map(([key, condition]) => ({
      title: condition.title || key,
      description: condition.description,
    }));
  }, [property]);

  return (
    <ControlLabels
      className={className}
      label={label}
      infoTooltipContent={
        (descriptionToDisplay || examples) && (
          <LabelInfo
            label={label}
            description={descriptionToDisplay}
            examples={examples}
            options={optionDescriptions}
          />
        )
      }
      requirement={getRequirement(property.isRequired, hideOptionalLabel)}
      htmlFor={htmlFor}
      format={format}
    >
      {children}
    </ControlLabels>
  );
};
