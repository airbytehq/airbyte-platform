import classNames from "classnames";
import React from "react";

import styles from "./RadioButtonTiles.module.scss";
import { SelectedIndicatorDot } from "./SelectedIndicatorDot";
import { Box } from "../../ui/Box";
import { Text } from "../../ui/Text";

export interface RadioButtonTileOption<T> {
  value: T;
  label: React.ReactNode;
  description: React.ReactNode;
  extra?: React.ReactNode;
  disabled?: boolean;
  tooltipContent?: React.ReactNode;
  "data-testid"?: string;
}

interface RadioButtonTileProps<T> {
  option: RadioButtonTileOption<T>;
  selectedValue: string;
  onSelectRadioButton: (value: T) => void;
  name: string;
  light?: boolean;
}

export const RadioButtonTile = <T extends string>({
  option,
  onSelectRadioButton,
  selectedValue,
  name,
  light,
}: RadioButtonTileProps<T>) => {
  const { value, label, description, extra, disabled, "data-testid": testId } = option;

  return (
    <div className={styles.radioButtonTiles__tile}>
      <input
        type="radio"
        name={name}
        disabled={disabled}
        id={`${name}-${value}`}
        value={value}
        checked={selectedValue === value}
        onChange={() => onSelectRadioButton(value)}
        className={styles.radioButtonTiles__hiddenInput}
        data-testid={testId ? `${testId}-option` : `radio-button-tile-${name}-${value}`}
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
    </div>
  );
};
