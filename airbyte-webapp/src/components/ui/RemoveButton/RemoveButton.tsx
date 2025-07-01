import classNames from "classnames";

import styles from "./RemoveButton.module.scss";
import { Icon } from "../Icon";

export const RemoveButton = ({
  onClick,
  className,
  disabled,
}: {
  onClick: () => void;
  className?: string;
  disabled?: boolean;
}) => {
  return (
    <button type="button" className={classNames(className, styles.removeButton)} onClick={onClick} disabled={disabled}>
      <Icon type="cross" />
    </button>
  );
};
