import classNames from "classnames";
import React, { useCallback } from "react";

import styles from "./ButtonTab.module.scss";
import { Badge } from "../Badge";
import { Text } from "../Text";

interface ButtonTabProps<T extends string = string> {
  id: T;
  name: string | React.ReactNode;
  isActive: boolean;
  disabled?: boolean;
  onSelect: (id: T) => void;
  badge?: string;
}

export const ButtonTab: React.FC<ButtonTabProps> = ({ name, id, isActive, onSelect, disabled, badge }) => {
  const onItemClickItem = useCallback(() => onSelect?.(id), [id, onSelect]);

  return (
    <button
      type="button"
      disabled={!onSelect || disabled}
      onClick={onItemClickItem}
      className={classNames(styles.tabContainer, {
        [styles["tabContainer--active"]]: isActive,
        [styles["tabContainer--inactive"]]: !isActive,
      })}
      data-id={`${id.toLowerCase()}-step`}
    >
      <Text color={isActive ? "darkBlue" : "grey"} className={styles.text} size="lg">
        {name}
      </Text>
      {badge && <Badge variant="blue">{badge}</Badge>}
    </button>
  );
};
