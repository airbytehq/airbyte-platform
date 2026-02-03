import { PropsWithChildren } from "react";

import styles from "./CreateConnectionFlowLayout.module.scss";

const Grid: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.createConnectionFlowLayout__grid}>{children}</div>;
};

const Header: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.createConnectionFlowLayout__header}>{children}</div>;
};

const Main: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.createConnectionFlowLayout__main}>{children}</div>;
};

const Footer: React.FC<PropsWithChildren> = ({ children }) => {
  return <div className={styles.createConnectionFlowLayout__footer}>{children}</div>;
};

export const CreateConnectionFlowLayout = {
  Grid,
  Header,
  Main,
  Footer,
};
