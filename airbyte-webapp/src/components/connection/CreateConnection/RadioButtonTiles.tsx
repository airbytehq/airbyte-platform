import classNames from "classnames";
import { ComponentProps } from "react";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./RadioButtonTiles.module.scss";
import { SelectedIndicatorDot } from "./SelectedIndicatorDot";

interface RadioButtonTilesOption<T> {
  value: T;
  label: React.ReactNode;
  description: React.ReactNode;
  extra?: React.ReactNode;
  disabled?: boolean;
}

interface RadioButtonTilesProps<T> {
  options: Array<RadioButtonTilesOption<T>>;
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
}: RadioButtonTilesProps<T>) => (
  <FlexContainer direction={direction}>
    {options.map(({ value, label, description, extra, disabled }) => (
      <FlexItem className={styles.radioButtonTiles__tile} key={value}>
        <input
          type="radio"
          name={name}
          disabled={disabled}
          id={`${name}-${value}`}
          value={value}
          checked={selectedValue === value}
          onChange={() => onSelectRadioButton(value)}
          className={styles.radioButtonTiles__hiddenInput}
          data-testid={`radio-button-tile-${name}-${value}`}
        />
        <label
          className={classNames(styles.radioButtonTiles__toggle, {
            [styles["radioButtonTiles__toggle--light"]]: light,
            [styles["radioButtonTiles__toggle--disabled"]]: disabled,
          })}
          htmlFor={`${name}-${value}`}
        >
          <div className={styles.radioButtonTiles__dot}>
            <SelectedIndicatorDot selected={selectedValue === value} />
          </div>
          <div>
            <Box mb="sm">
              <Text size="lg" color={disabled ? "grey" : "darkBlue"}>
                {label}
              </Text>
            </Box>
            <Text size="sm" color={disabled ? "grey" : "darkBlue"}>
              {description}
            </Text>
            {extra && <Box mt="sm">{extra}</Box>}
          </div>
        </label>
      </FlexItem>
    ))}
  </FlexContainer>
);
