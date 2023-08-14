import React from "react";
import { useController, useFormContext } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { DropDown } from "components/ui/DropDown";

import { getLabelAndTooltip } from "./manifestHelpers";

interface OneOfType {
  type: string;
}

interface Option<T extends OneOfType> {
  label: string;
  value: string;
  default?: T;
}

export interface OneOfOption<T extends OneOfType> {
  label: string; // label shown in the dropdown menu
  default: T; // default values for the path
  children?: React.ReactNode;
}

interface BuilderOneOfProps<T extends OneOfType> {
  options: Array<OneOfOption<T>>;
  path: string; // path to the oneOf component in the json schema
  label?: string;
  tooltip?: string | React.ReactNode;
  manifestPath?: string;
  manifestOptionPaths?: string[];
  omitInterpolationContext?: boolean;
  onSelect?: (type: string) => void;
}

export const BuilderOneOf = <T extends OneOfType>({
  options,
  label,
  tooltip,
  path,
  manifestPath,
  manifestOptionPaths,
  omitInterpolationContext,
  onSelect,
}: BuilderOneOfProps<T>) => {
  const { setValue, unregister } = useFormContext();
  const { field } = useController({ name: `${path}.type` });

  const selectedOption = options.find((option) => option.default.type === field.value);
  const { label: finalLabel, tooltip: finalTooltip } = getLabelAndTooltip(
    label,
    tooltip,
    manifestPath,
    path,
    false,
    omitInterpolationContext,
    manifestOptionPaths
  );

  return (
    <GroupControls
      label={<ControlLabels label={finalLabel} infoTooltipContent={finalTooltip} />}
      control={
        <DropDown
          {...field}
          options={options.map((option) => {
            return { label: option.label, value: option.default.type, default: option.default };
          })}
          value={field.value ?? options[0].default.type}
          onChange={(selectedOption: Option<T>) => {
            if (selectedOption.value === field.value) {
              return;
            }
            // clear all values for this oneOf and set selected option and default values
            unregister(path, { keepValue: true, keepDefaultValue: true });
            setValue(path, selectedOption.default);

            onSelect?.(selectedOption.value);
          }}
        />
      }
    >
      {selectedOption?.children}
    </GroupControls>
  );
};
