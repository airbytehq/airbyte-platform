import React, { useRef, useEffect } from "react";
import { useFormContext } from "react-hook-form";

import GroupControls from "components/GroupControls";
import { ControlLabels } from "components/LabeledControl";
import { ListBox } from "components/ui/ListBox";

import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import { getLabelAndTooltip } from "./manifestHelpers";
import { useWatchWithPreview } from "../preview";

interface OneOfType {
  type: string;
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
  onSelect?: (type: string) => void;
}

export const BuilderOneOf = <T extends OneOfType>({
  options,
  label,
  tooltip,
  path,
  manifestPath,
  manifestOptionPaths,
  onSelect,
}: BuilderOneOfProps<T>) => {
  const { setValue, unregister } = useFormContext();
  const fieldName = `${path}.type`;
  // Use value from useWatch instead of from useController, since the former will respect updates made to parent paths from setValue
  const { fieldValue, isPreview } = useWatchWithPreview({ name: fieldName });

  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  const elementRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, fieldName);
  }, [handleScrollToField, fieldName]);

  const selectedOption = options.find((option) => option.default.type === fieldValue);
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
      ref={elementRef}
      label={<ControlLabels label={finalLabel} infoTooltipContent={finalTooltip} />}
      control={
        <ListBox
          options={options.map((option) => ({
            label: option.label,
            value: option,
          }))}
          placement="bottom-end"
          adaptiveWidth={false}
          isDisabled={isPreview}
          selectedValue={selectedOption ?? options[0]}
          onSelect={(selectedOption: OneOfOption<T>) => {
            if (selectedOption.default.type === fieldValue) {
              return;
            }
            // clear all values for this oneOf and set selected option and default values
            unregister(path, { keepValue: true, keepDefaultValue: true });
            setValue(path, selectedOption.default);

            onSelect?.(selectedOption.default.type);
          }}
          data-testid={fieldName}
        />
      }
    >
      {selectedOption?.children}
    </GroupControls>
  );
};
