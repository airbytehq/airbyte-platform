import { ComponentProps } from "react";

import { FlexContainer } from "components/ui/Flex";

import { RadioButtonTile, RadioButtonTileOption } from "./RadioButtonTile";

interface RadioButtonTilesProps<T> {
  options: Array<RadioButtonTileOption<T>>;
  selectedValue: string;
  onSelectRadioButton: (value: T) => void;
  name: string;
  direction?: ComponentProps<typeof FlexContainer>["direction"];
  light?: boolean;
}

export const RadioButtonTiles = <T extends string>({
  options,
  onSelectRadioButton,
  selectedValue,
  name,
  direction,
  light,
}: RadioButtonTilesProps<T>) => {
  const radioButtonTile = (option: RadioButtonTileOption<T>) => (
    <RadioButtonTile
      key={option.value}
      option={option}
      selectedValue={selectedValue}
      onSelectRadioButton={onSelectRadioButton}
      name={name}
      light={light}
    />
  );
  return <FlexContainer direction={direction}>{options.map((option) => radioButtonTile(option))}</FlexContainer>;
};
