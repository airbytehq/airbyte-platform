import React from "react";

import { FlexContainer } from "components/ui/Flex";

import styles from "./Auth.module.scss";

export const AuthLayout: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <FlexContainer className={styles.container}>
      <FlexContainer
        direction="column"
        alignItems="center"
        justifyContent="center"
        className={styles["container__left-side"]}
      >
        {children}
      </FlexContainer>
    </FlexContainer>
  );
};
