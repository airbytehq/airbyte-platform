import classnames from "classnames";
import React from "react";

import styles from "./Spinner.module.scss";

interface SpinnerProps {
  size?: "sm" | "xs" | "md";
}

export const Spinner: React.FC<SpinnerProps> = ({ size }) => (
  <div className={classnames(styles.spinner, { [styles.small]: size === "sm", [styles.extraSmall]: size === "xs" })} />
);
