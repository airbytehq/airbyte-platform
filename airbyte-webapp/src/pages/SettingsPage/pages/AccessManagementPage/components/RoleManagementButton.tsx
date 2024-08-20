import React from "react";

import styles from "./RoleManagementButton.module.scss";

export const RoleManagementButton = React.forwardRef<
  HTMLButtonElement | null,
  React.ButtonHTMLAttributes<HTMLButtonElement>
>(({ children, ...props }, ref?) => {
  return (
    <button className={styles.roleManagementButton} ref={ref} {...props}>
      {children}
    </button>
  );
});

RoleManagementButton.displayName = "RoleManagementButton";
