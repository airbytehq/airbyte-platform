import { Radio } from "@headlessui/react";
import classNames from "classnames";
import React from "react";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./RadioButtonTiles.module.scss";

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
  name: string;
  light?: boolean;
}

export const RadioButtonTile = <T extends string>({ option, name, light }: RadioButtonTileProps<T>) => {
  const { value, label, description, extra, disabled, "data-testid": testId, tooltipContent } = option;

  return (
    <Radio
      value={value}
      disabled={disabled}
      className={styles.radioButtonTiles__tile}
      data-testid={testId ? `${testId}-option` : `radio-button-tile-${name}-${value}`}
    >
      {({ checked }) => {
        const content = (
          <FlexContainer
            className={classNames(styles.radioButtonTiles__toggle, {
              [styles["radioButtonTiles__toggle--light"]]: light,
              [styles["radioButtonTiles__toggle--disabled"]]: disabled,
            })}
          >
            <FlexItem noShrink grow={false}>
              <RadioButton name={name} disabled={disabled} id={`${name}-${value}`} value={value} checked={checked} />
            </FlexItem>
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
          </FlexContainer>
        );

        return tooltipContent ? <Tooltip control={content}>{tooltipContent}</Tooltip> : content;
      }}
    </Radio>
  );
};
