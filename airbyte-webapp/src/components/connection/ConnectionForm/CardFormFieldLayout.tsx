import React, { PropsWithChildren } from "react";

import styles from "./CardFormFieldLayout.module.scss";

/**
 * react-hook-form form control layout component
 * will replace FormFieldLayout in future
 * @see FormFieldLayout
 * @param children
 * @constructor
 */
export const CardFormFieldLayout: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return <div className={styles.container}>{children}</div>;
};
