import React from "react";
import { useIntl } from "react-intl";
import { useSearchParams } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";

import { useExperiment } from "hooks/services/Experiment";
import { RoutePaths } from "pages/routePaths";

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
  const [searchParams] = useSearchParams();
  const { formatMessage } = useIntl();
  const rightSideUrl = useExperiment("authPage.rightSideUrl");
  const embeddedRightSideUrl = useExperiment("authPage.embedded.rightSideUrl");

  const loginRedirect = searchParams.get("loginRedirect");
  const shouldShowEmbedded = loginRedirect?.includes(RoutePaths.EmbeddedOnboarding);
  const rightSideUrlToUse = shouldShowEmbedded ? embeddedRightSideUrl : rightSideUrl;

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
        {hasValidRightSideUrl(rightSideUrlToUse) ? (
          <iframe
            className={styles.container__iframe}
            src={rightSideUrlToUse}
            title={formatMessage({ id: "login.rightSideFrameTitle" })}
          />
        ) : (
          <PersonQuoteCover />
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
