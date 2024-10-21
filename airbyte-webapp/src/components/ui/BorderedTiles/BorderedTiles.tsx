import classNames from "classnames";
import { PropsWithChildren } from "react";

import styles from "./BorderedTiles.module.scss";

export const BorderedTiles: React.FC<PropsWithChildren<{ className?: string }>> = ({ children, className }) => {
  return <div className={classNames(className, styles.borderedTiles)}>{children}</div>;
};

export const BorderedTile: React.FC<PropsWithChildren<{ className?: string }>> = ({ children, className }) => {
  return <div className={classNames(className, styles.borderedTiles__tile)}>{children}</div>;
};
