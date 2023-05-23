import React, { useMemo } from "react";

import { LabelInfo } from "components/Label";
import { ControlLabels } from "components/LabeledControl";

import { FormBlock } from "core/form/types";

interface PropertyLabelProps {
  property: FormBlock;
  label: React.ReactNode;
  description?: string;
  optional?: boolean;
  className?: string;
  htmlFor?: string;
  format?: React.ReactNode;
}

export const PropertyLabel: React.FC<React.PropsWithChildren<PropertyLabelProps>> = ({
  property,
  label,
  description,
  optional,
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
      optional={optional ?? !property.isRequired}
      htmlFor={htmlFor}
      format={format}
    >
      {children}
    </ControlLabels>
  );
};
