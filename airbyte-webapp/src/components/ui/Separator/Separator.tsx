import classNames from "classnames";

import styles from "./Separator.module.scss";

interface SeparatorProps {
  className?: string;
}

export const Separator: React.FC<SeparatorProps> = ({ className }) => {
  return <hr className={classNames(styles.separator, className)} />;
};
