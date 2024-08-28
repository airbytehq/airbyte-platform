import classNames from "classnames";

import styles from "./LoadingSkeleton.module.scss";

export const LoadingSkeleton = ({ className }: { className?: string }) => {
  return <div className={classNames(styles.loadingSkeleton, styles["loadingSkeleton--button"], className)} />;
};
