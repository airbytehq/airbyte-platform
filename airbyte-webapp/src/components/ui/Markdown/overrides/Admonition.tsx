import classNames from "classnames";
import { ReactNode } from "react";

// Since we're dynamically accessing the admonition--{node.name} classes, the linter
// can't determine that those are used, thus we need to ignore unused classes here.
// eslint-disable-next-line css-modules/no-unused-class
import styles from "./Admonition.module.scss";

export const Admonition = ({ children, type }: { children: ReactNode; type: string }) => {
  return (
    <div className={classNames(styles.admonition, styles[`admonition--${type}` as keyof typeof styles])}>
      {children}
    </div>
  );
};
