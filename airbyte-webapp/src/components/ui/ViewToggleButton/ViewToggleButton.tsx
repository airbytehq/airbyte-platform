import classnames from "classnames";
import React from "react";

import { Text } from "components/ui/Text";

import styles from "./ViewToggleButton.module.scss";

interface ViewToggleButtonProps {
  leftLabel: string;
  rightLabel: string;
  isRightSelected: boolean;
  onClick: () => void;
  className?: string;
}

export const ViewToggleButton: React.FC<ViewToggleButtonProps> = ({
  leftLabel,
  rightLabel,
  isRightSelected,
  onClick,
  className,
}) => {
  return (
    <button type="button" className={classnames(styles.button, className)} onClick={onClick}>
      <Text className={classnames(styles.text, !isRightSelected ? styles.selected : styles.unselected)} bold>
        {leftLabel}
      </Text>
      <Text className={classnames(styles.text, isRightSelected ? styles.selected : styles.unselected)} bold>
        {rightLabel}
      </Text>
    </button>
  );
};
