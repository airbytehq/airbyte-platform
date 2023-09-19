import React, { PropsWithChildren } from "react";

import styles from "./HookFormFieldLayout.module.scss";

/**
 * react-hook-form form control layout component
 * will replace FormFieldLayout in future
 * @see FormFieldLayout
 * @param children
 * @constructor
 */
export const HookFormFieldLayout: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return <div className={styles.container}>{children}</div>;
};
