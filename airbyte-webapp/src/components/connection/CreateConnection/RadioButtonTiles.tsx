import classNames from "classnames";
import { ComponentProps } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./RadioButtonTiles.module.scss";
import { SelectedIndicatorDot } from "./SelectedIndicatorDot";

interface RadioButtonTilesOption<T> {
  value: T;
  label: string;
  labelValues?: ComponentProps<typeof FormattedMessage>["values"];
  description: string;
  extra?: React.ReactNode;
  disabled?: boolean;
}

interface RadioButtonTilesProps<T> {
  options: Array<RadioButtonTilesOption<T>>;
  selectedValue: string;
  onSelectRadioButton: (value: T) => void;
  name: string;
  direction?: ComponentProps<typeof FlexContainer>["direction"];
}

export const RadioButtonTiles = <T extends string>({
  options,
  onSelectRadioButton,
  selectedValue,
  name,
  direction,
}: RadioButtonTilesProps<T>) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer direction={direction}>
      {options.map((option) => (
        <FlexItem className={styles.radioButtonTiles__tile} key={option.value}>
          <input
            type="radio"
            name={name}
            disabled={option.disabled}
            id={`${name}-${option.value}`}
            value={option.value}
            checked={selectedValue === option.value}
            onChange={() => onSelectRadioButton(option.value)}
            className={styles.radioButtonTiles__hiddenInput}
            data-testid={`radio-button-tile-${name}-${option.value}`}
          />
          <label
            className={classNames(styles.radioButtonTiles__toggle, {
              [styles["radioButtonTiles__toggle--disabled"]]: option.disabled,
            })}
            htmlFor={`${name}-${option.value}`}
          >
            <div className={styles.radioButtonTiles__dot}>
              <SelectedIndicatorDot selected={selectedValue === option.value} />
            </div>
            <div>
              <Box mb="sm">
                <Text size="lg" color={option.disabled ? "grey" : "darkBlue"}>
                  {formatMessage({ id: option.label }, option.labelValues)}
                </Text>
              </Box>
              <Text size="sm" color={option.disabled ? "grey" : "darkBlue"}>
                {formatMessage({ id: option.description })}
              </Text>
              {option.extra && <Box mt="sm">{option.extra}</Box>}
            </div>
          </label>
        </FlexItem>
      ))}
    </FlexContainer>
  );
};
