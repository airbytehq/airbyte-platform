import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./RadioButtonTiles.module.scss";
import { SelectedIndicatorDot } from "./SelectedIndicatorDot";

interface RadioButtonTilesOption<T> {
  value: T;
  label: string;
  description: string;
  disabled?: boolean;
}

interface RadioButtonTilesProps<T> {
  options: Array<RadioButtonTilesOption<T>>;
  selectedValue: string;
  onSelectRadioButton: (value: T) => void;
  name: string;
}

export const RadioButtonTiles = <T extends string>({
  options,
  onSelectRadioButton,
  selectedValue,
  name,
}: RadioButtonTilesProps<T>) => {
  return (
    <FlexContainer>
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
                  <FormattedMessage id={option.label} />
                </Text>
              </Box>
              <Text size="sm" color={option.disabled ? "grey" : "darkBlue"}>
                <FormattedMessage id={option.description} />
              </Text>
            </div>
          </label>
        </FlexItem>
      ))}
    </FlexContainer>
  );
};
