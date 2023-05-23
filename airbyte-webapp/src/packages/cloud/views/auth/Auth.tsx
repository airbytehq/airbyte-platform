import React, { Suspense, useEffect } from "react";
import { useIntl } from "react-intl";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";

import { LoadingPage } from "components";
import { FlexContainer } from "components/ui/Flex";

import { useConfig } from "config";
import { useExperiment } from "hooks/services/Experiment";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import { FirebaseActionRoute } from "packages/cloud/views/FirebaseActionRoute";
import { loadFathom } from "utils/fathom";

import styles from "./Auth.module.scss";

const PersonQuoteCover = React.lazy(() => import("./components/PersonQuoteCover"));
const LoginPage = React.lazy(() => import("./LoginPage"));
const ResetPasswordPage = React.lazy(() => import("./ResetPasswordPage"));
const SignupPage = React.lazy(() => import("./SignupPage"));

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

export const Auth: React.FC = () => {
  const { pathname } = useLocation();
  const { formatMessage } = useIntl();
  const { loggedOut } = useAuthService();
  const rightSideUrl = useExperiment("authPage.rightSideUrl", undefined);

  const config = useConfig();
  useEffect(() => {
    loadFathom(config.fathomSiteId);
  }, [config.fathomSiteId]);

  return (
    <FlexContainer className={styles.container}>
      <FlexContainer
        direction="column"
        alignItems="center"
        justifyContent="center"
        className={styles["container__left-side"]}
      >
        <Suspense fallback={<LoadingPage />}>
          <Routes>
            <Route path={CloudRoutes.Login} element={<LoginPage />} />
            <Route path={CloudRoutes.Signup} element={<SignupPage />} />
            <Route path={CloudRoutes.ResetPassword} element={<ResetPasswordPage />} />
            <Route path={CloudRoutes.FirebaseAction} element={<FirebaseActionRoute />} />
            <Route
              path="*"
              element={
                <Navigate
                  to={`${CloudRoutes.Login}${
                    loggedOut && pathname.includes("/settings/account") ? "" : `?from=${pathname}`
                  }`}
                />
              }
            />
          </Routes>
        </Suspense>
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
