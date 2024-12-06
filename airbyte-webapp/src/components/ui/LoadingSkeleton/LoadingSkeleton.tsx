import classNames from "classnames";

import styles from "./LoadingSkeleton.module.scss";

export type LoadingSkeletonVariants = "shimmer" | "magic";

export interface LoadingSkeletonProps {
  className?: string;
  variant?: LoadingSkeletonVariants;
}

export const LoadingSkeleton = ({ className, variant = "shimmer" }: LoadingSkeletonProps) => {
  const variantStyles = {
    [styles.loadingSkeletonShimmer]: variant === "shimmer",
    [styles.loadingSkeletonMagic]: variant === "magic",
  };

  const classes = classNames(styles.loadingSkeleton, styles["loadingSkeleton--buttonHeight"], variantStyles, className);

  return <div className={classes} />;
};
