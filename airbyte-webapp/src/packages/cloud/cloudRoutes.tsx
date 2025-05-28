import React, { Suspense, useMemo } from "react";
import { createSearchParams, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useEffectOnce } from "react-use";

import LoadingPage from "components/LoadingPage";

import MainLayout from "area/layout/MainLayout";
import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useInvalidateAllWorkspaceScopeOnChange } from "core/api";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { useAuthService } from "core/services/auth";
import { storeConnectorChatBuilderFromQuery } from "core/utils/connectorChatBuilderStorage";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useExperiment } from "hooks/services/Experiment";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useQuery } from "hooks/useQuery";
import { EmbeddedSourceCreatePage } from "pages/embedded/EmbeddedSourceCreatePage/EmbeddedSourcePage";
import { OrganizationRoutes } from "pages/organization/OrganizationRoutes";
import { RoutePaths } from "pages/routePaths";
import { WorkspacesPage } from "pages/workspaces/WorkspacesPage";

import { AcceptInvitation } from "./AcceptInvitation";
import { CloudRoutes } from "./cloudRoutePaths";
import { LDExperimentServiceProvider } from "./services/thirdParty/launchdarkly";
import { SSOBookmarkPage } from "./views/auth/SSOBookmarkPage";
import { SSOIdentifierPage } from "./views/auth/SSOIdentifierPage";
import { WorkspacesRoutes } from "./views/routes/WorkspacesRoutes";

// Lazy loaded components
const LoginPage = React.lazy(() => import("./views/auth/LoginPage"));
const SignupPage = React.lazy(() => import("./views/auth/SignupPage"));
const CloudMainView = React.lazy(() => import("packages/cloud/views/layout/CloudMainView"));
const AuthLayout = React.lazy(() => import("packages/cloud/views/auth"));
const DefaultView = React.lazy(() => import("pages/DefaultView"));

const CloudMainViewRoutes = () => {
  const { loginRedirect } = useQuery<{ loginRedirect: string }>();
  const isOrgPickerEnabled = useExperiment("sidebar.showOrgPicker");

  if (loginRedirect) {
    return <Navigate to={loginRedirect} replace />;
  }

  return (
    <Routes>
      <Route path={CloudRoutes.AcceptInvitation} element={<AcceptInvitation />} />

      {isOrgPickerEnabled ? (
        <Route element={<MainLayout />}>
          <Route path="account" element={<div>User Settings</div>} />
          <Route path={`${RoutePaths.Organization}/:organizationId/*`} element={<OrganizationRoutes />} />
          <Route path={`${RoutePaths.Workspaces}/:workspaceId/*`} element={<WorkspacesRoutes />} />
        </Route>
      ) : (
        <>
          <Route path={RoutePaths.Workspaces} element={<WorkspacesPage />} />
          <Route element={<CloudMainView />}>
            <Route path={`${RoutePaths.Workspaces}/:workspaceId/*`} element={<WorkspacesRoutes />} />
          </Route>
        </>
      )}
      <Route path="*" element={<DefaultView />} />
    </Routes>
  );
};

export const Routing: React.FC = () => {
  const { user, inited, provider, loggedOut } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();
  const { pathname: originalPathname, search, hash } = useLocation();

  useEffectOnce(() => {
    storeConnectorChatBuilderFromQuery(search);
  });

  const loginRedirectSearchParam = `${createSearchParams({
    loginRedirect: `${originalPathname}${search}${hash}`,
  })}`;

  const loginRedirectTo =
    loggedOut && originalPathname === "/"
      ? { pathname: CloudRoutes.Login }
      : { pathname: CloudRoutes.Login, search: loginRedirectSearchParam };

  useBuildUpdateCheck();

  // invalidate everything in the workspace scope when the workspaceId changes
  useInvalidateAllWorkspaceScopeOnChange(workspaceId);

  const analyticsContext = useMemo(
    () =>
      user
        ? {
            cloud_user_id: user.userId,
          }
        : null,
    [user]
  );

  const userTraits = useMemo(
    () =>
      user
        ? {
            provider,
            email: user.email,
            isCorporate: isCorporateEmail(user.email),
            currentWorkspaceId: workspaceId,
          }
        : {},
    [provider, user, workspaceId]
  );

  useEffectOnce(() => {
    storeUtmFromQuery(search);
  });

  useAnalyticsRegisterValues(analyticsContext);
  useAnalyticsIdentifyUser(user?.userId, userTraits);

  if (!inited) {
    // Using <LoadingPage /> here causes flickering, because Suspense will immediately render it again
    return null;
  }

  return (
    <LDExperimentServiceProvider>
      <Suspense fallback={<LoadingPage />}>
        <Routes>
          <Route path={`/${RoutePaths.EmbeddedWidget}`} element={<EmbeddedSourceCreatePage />} />
          <Route
            path="*"
            element={
              <>
                {/* All routes for non logged in users */}
                {!user && (
                  <AuthLayout>
                    <Suspense fallback={<LoadingPage />}>
                      <Routes>
                        <Route path={CloudRoutes.SsoBookmark} element={<SSOBookmarkPage />} />
                        <Route path={CloudRoutes.Sso} element={<SSOIdentifierPage />} />
                        <Route path={CloudRoutes.Login} element={<LoginPage />} />
                        <Route path={CloudRoutes.Signup} element={<SignupPage />} />
                        {/* In case a not logged in user tries to access anything else navigate them to login */}
                        <Route path="*" element={<Navigate to={loginRedirectTo} />} />
                      </Routes>
                    </Suspense>
                  </AuthLayout>
                )}
                {/* Allow all regular routes if the user is logged in */}
                {user && <CloudMainViewRoutes />}
              </>
            }
          />
        </Routes>
      </Suspense>
    </LDExperimentServiceProvider>
  );
};
