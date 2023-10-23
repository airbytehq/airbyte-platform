import React from "react";
import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";

import { useExperiment } from "hooks/services/Experiment";

import styles from "./Auth.module.scss";

const PersonQuoteCover = React.lazy(() => import("./components/PersonQuoteCover"));

const hasValidRightSideUrl = (url?: string): boolean => {
  if (url) {
    try {
      const parsedUrl = new URL(url);
      const isValid = parsedUrl.protocol === "https:" && parsedUrl.hostname.endsWith("airbyte.com");
      if (!isValid) {
        console.warn(`${parsedUrl} is not valid.`);
      }
      return isValid;
    } catch (e) {
      console.warn(e);
    }
  }

  return false;
};

export const AuthLayout: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const rightSideUrl = useExperiment("authPage.rightSideUrl", undefined);

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
      <FlexContainer direction="column" className={styles["container__right-side"]}>
        {hasValidRightSideUrl(rightSideUrl) ? (
          <iframe
            className={styles.container__iframe}
            src={rightSideUrl}
            title={formatMessage({ id: "login.rightSideFrameTitle" })}
          />
        ) : (
          <PersonQuoteCover />
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
