import classnames from "classnames";
import { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./UiYamlToggleButton.module.scss";

interface UiYamlToggleButtonProps {
  className?: string;
  yamlSelected: boolean;
  onClick: () => void;
  size: "xs" | "sm";
  disabled?: boolean;
  tooltip?: ReactNode;
}

export const UiYamlToggleButton: React.FC<UiYamlToggleButtonProps> = ({
  className,
  yamlSelected,
  onClick,
  size,
  disabled,
  tooltip,
}) => {
  const sizeStyles = {
    [styles.xs]: size === "xs",
    [styles.sm]: size === "sm",
  };

  const toggleButton = (
    <button
      type="button"
      className={classnames(styles.button, className, sizeStyles)}
      onClick={onClick}
      disabled={disabled}
    >
      <Text
        className={classnames(styles.text, {
          ...sizeStyles,
          [styles.selected]: !yamlSelected,
          [styles.unselected]: yamlSelected,
        })}
        bold
      >
        <FormattedMessage id="connectorBuilder.uiYamlToggle.ui" />
      </Text>
      <Text
        className={classnames(styles.text, {
          ...sizeStyles,
          [styles.selected]: yamlSelected,
          [styles.unselected]: !yamlSelected,
        })}
        bold
      >
        <FormattedMessage id="connectorBuilder.uiYamlToggle.yaml" />
      </Text>
    </button>
  );

  return tooltip ? (
    <Tooltip containerClassName={styles.tooltipContainer} control={toggleButton}>
      {tooltip}
    </Tooltip>
  ) : (
    toggleButton
  );
};
