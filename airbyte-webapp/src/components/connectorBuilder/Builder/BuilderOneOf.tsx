import React from "react";
import { useController, useFormContext } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { DropDown } from "components/ui/DropDown";

import { getLabelAndTooltip } from "./manifestHelpers";

interface Option {
  label: string;
  value: string;
  default?: object;
}

export interface OneOfOption {
  label: string; // label shown in the dropdown menu
  typeValue: string; // value to set on the `type` field for this component - should match the oneOf type definition
  default: object; // default values for the path
  children?: React.ReactNode;
}

interface BuilderOneOfProps {
  options: OneOfOption[];
  path: string; // path to the oneOf component in the json schema
  label?: string;
  tooltip?: string | React.ReactNode;
  manifestPath?: string;
  manifestOptionPaths?: string[];
  onSelect?: (type: string) => void;
}

export const BuilderOneOf: React.FC<BuilderOneOfProps> = ({
  options,
  label,
  tooltip,
  path,
  manifestPath,
  manifestOptionPaths,
  onSelect,
}) => {
  const { setValue, unregister } = useFormContext();
  const { field } = useController({ name: `${path}.type` });

  const selectedOption = options.find((option) => option.typeValue === field.value);
  const { label: finalLabel, tooltip: finalTooltip } = getLabelAndTooltip(
    label,
    tooltip,
    manifestPath,
    path,
    false,
    manifestOptionPaths
  );

  return (
    <GroupControls
      label={<ControlLabels label={finalLabel} infoTooltipContent={finalTooltip} />}
      control={
        <DropDown
          {...field}
          options={options.map((option) => {
            return { label: option.label, value: option.typeValue, default: option.default };
          })}
          value={field.value ?? options[0].typeValue}
          onChange={(selectedOption: Option) => {
            if (selectedOption.value === field.value) {
              return;
            }
            unregister(path, { keepValue: true, keepDefaultValue: true });
            // clear all values for this oneOf and set selected option and default values
            setValue(path, {
              type: selectedOption.value,
              ...selectedOption.default,
            });

            onSelect?.(selectedOption.value);
          }}
        />
      }
    >
      {selectedOption?.children}
    </GroupControls>
  );
};
