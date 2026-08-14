import classNames from "classnames";

import styles from "./RemoveButton.module.scss";
import { Icon } from "../Icon";

export const RemoveButton = ({
  onClick,
  className,
  disabled,
  "aria-label": ariaLabel,
}: {
  onClick: () => void;
  className?: string;
  disabled?: boolean;
  "aria-label"?: string;
}) => {
  return (
    <button
      type="button"
      className={classNames(className, styles.removeButton)}
      onClick={onClick}
      disabled={disabled}
      aria-label={ariaLabel}
    >
      <Icon type="cross" />
    </button>
  );
};
