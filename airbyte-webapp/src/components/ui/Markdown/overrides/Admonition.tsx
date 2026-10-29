import classNames from "classnames";
import MarkdownToJsx from "markdown-to-jsx";
import { ReactNode } from "react";

// Since we're dynamically accessing the admonition--{node.name} classes, the linter
// can't determine that those are used, thus we need to ignore unused classes here.
// eslint-disable-next-line css-modules/no-unused-class
import styles from "./Admonition.module.scss";

export const Admonition = ({ children, type, title }: { children: ReactNode; type: string; title?: string }) => {
  return (
    <div className={classNames(styles.admonition, styles[`admonition--${type}` as keyof typeof styles])}>
      {title && (
        <div className={styles.admonition__title}>
          <MarkdownToJsx options={{ forceInline: true }}>{title}</MarkdownToJsx>
        </div>
      )}
      {children}
    </div>
  );
};
