import classNames from "classnames";

import { Text } from "components/ui/Text";

import { getTextColorForBackground, isSafeHexValue } from "core/utils/color";

import styles from "./TagBadge.module.scss";

export interface TagProps {
  color?: string;
  text: string;
}

export const TagBadge: React.FC<TagProps> = ({ color, text }) => {
  const hasValidBackgroundColor = color && isSafeHexValue(color);
  const textColorClass =
    hasValidBackgroundColor && getTextColorForBackground(color) === "light"
      ? styles["tagBadge__text--light"]
      : styles["tagBadge__text--dark"];

  return (
    <span
      className={classNames(styles.tagBadge, { [styles["tagBadge--fallback"]]: !hasValidBackgroundColor })}
      style={{
        backgroundColor: hasValidBackgroundColor ? `#${color}` : undefined,
      }}
    >
      <Text size="sm" className={textColorClass}>
        {text}
      </Text>
    </span>
  );
};
