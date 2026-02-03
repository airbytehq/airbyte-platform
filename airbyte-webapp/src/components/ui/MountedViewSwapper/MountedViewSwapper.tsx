import React from "react";

import styles from "./MountedViewSwapper.module.scss";

interface MountedViewSwapperProps {
  /** The view to show when isFirstView is true */
  firstView: React.ReactNode;
  /** The view to show when isFirstView is false */
  secondView: React.ReactNode;
  /** Controls which view is visible */
  isFirstView: boolean;
}

/**
 * A component that renders two views and swaps between them while keeping both mounted.
 * This is useful for preserving state when toggling between views.
 */
export const MountedViewSwapper: React.FC<MountedViewSwapperProps> = ({ firstView, secondView, isFirstView }) => {
  return (
    <div className={styles.wrapper}>
      {/* First View - kept mounted but hidden when not active */}
      <div className={`${styles.viewContainer} ${isFirstView ? styles.visible : styles.hidden}`}>{firstView}</div>

      {/* Second View - kept mounted but hidden when not active */}
      <div className={`${styles.viewContainer} ${!isFirstView ? styles.visible : styles.hidden}`}>{secondView}</div>
    </div>
  );
};
