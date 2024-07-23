import classNames from "classnames";

import styles from "./RemoveButton.module.scss";
import { Icon } from "../Icon";

export const RemoveButton = ({ onClick, className }: { onClick: () => void; className?: string }) => {
  return (
    <button type="button" className={classNames(className, styles.removeButton)} onClick={onClick}>
      <Icon type="cross" />
    </button>
  );
};
