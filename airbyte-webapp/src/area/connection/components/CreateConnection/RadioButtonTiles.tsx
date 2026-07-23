import { RadioGroup } from "@headlessui/react";
import { ComponentProps } from "react";

import { FlexContainer } from "components/ui/Flex";

import { RadioButtonTile, RadioButtonTileOption } from "./RadioButtonTile";

interface RadioButtonTilesProps<T> {
  options: Array<RadioButtonTileOption<T>>;
  selectedValue: string;
  onSelectRadioButton: (value: T) => void;
  name: string;
  direction?: ComponentProps<typeof FlexContainer>["direction"];
  /** When true, applies light styling variant with no border and reduced padding */
  light?: boolean;
}

export const RadioButtonTiles = <T extends string>({
  options,
  selectedValue,
  onSelectRadioButton,
  name,
  direction,
  light,
}: RadioButtonTilesProps<T>) => {
  return (
    <RadioGroup value={selectedValue} onChange={onSelectRadioButton}>
      <FlexContainer direction={direction}>
        {options.map((option) => (
          <RadioButtonTile key={option.value} option={option} name={name} light={light} />
        ))}
      </FlexContainer>
    </RadioGroup>
  );
};
