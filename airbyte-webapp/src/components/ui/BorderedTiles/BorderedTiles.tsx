import { PropsWithChildren } from "react";

import styles from "./BorderedTiles.module.scss";

export const BorderedTiles: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.borderedTiles}>{children}</div>;
};

export const BorderedTile: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.borderedTiles__tile}>{children}</div>;
};
