import React, { PropsWithChildren, Suspense, useMemo } from "react";
import { createSearchParams, Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useEffectOnce } from "react-use";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import LoadingPage from "components/LoadingPage";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { useCurrentWorkspace, useInvalidateAllWorkspaceScopeOnChange } from "core/api";
import { usePrefetchCloudWorkspaceData } from "core/api/cloud";
import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { useAuthService } from "core/services/auth";
import { isCorporateEmail } from "core/utils/freeEmailProviders";
import { storeUtmFromQuery } from "core/utils/utmStorage";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useQuery } from "hooks/useQuery";
import ConnectorBuilderRoutes from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, DestinationPaths, SourcePaths } from "pages/routePaths";
import { CompleteOauthRequest } from "views/CompleteOauthRequest";

import { CloudRoutes } from "./cloudRoutePaths";
import { LDExperimentServiceProvider } from "./services/thirdParty/launchdarkly";
import { SSOBookmarkPage } from "./views/auth/SSOBookmarkPage";
import { SSOIdentifierPage } from "./views/auth/SSOIdentifierPage";
import { FirebaseActionRoute } from "./views/FirebaseActionRoute";

const LoginPage = React.lazy(() => import("./views/auth/LoginPage"));
const ResetPasswordPage = React.lazy(() => import("./views/auth/ResetPasswordPage"));
const SignupPage = React.lazy(() => import("./views/auth/SignupPage"));
const CloudMainView = React.lazy(() => import("packages/cloud/views/layout/CloudMainView"));
const CloudWorkspacesPage = React.lazy(() => import("packages/cloud/views/workspaces"));
const AuthLayout = React.lazy(() => import("packages/cloud/views/auth"));
const BillingPage = React.lazy(() => import("packages/cloud/views/billing"));
const UpcomingFeaturesPage = React.lazy(() => import("packages/cloud/views/UpcomingFeaturesPage"));
const SpeakeasyRedirectPage = React.lazy(() => import("pages/SpeakeasyRedirectPage"));

const ConnectionsRoutes = React.lazy(() => import("pages/connections/ConnectionsRoutes"));

const AllDestinationsPage = React.lazy(() => import("pages/destination/AllDestinationsPage"));
const CreateDestinationPage = React.lazy(() => import("pages/destination/CreateDestinationPage"));
const SelectDestinationPage = React.lazy(() => import("pages/destination/SelectDestinationPage"));
const DestinationItemPage = React.lazy(() => import("pages/destination/DestinationItemPage"));
const DestinationConnectionsPage = React.lazy(() => import("pages/destination/DestinationConnectionsPage"));
const DestinationSettingsPage = React.lazy(() => import("pages/destination/DestinationSettingsPage"));

const AllSourcesPage = React.lazy(() => import("pages/source/AllSourcesPage"));
const CreateSourcePage = React.lazy(() => import("pages/source/CreateSourcePage"));
const SelectSourcePage = React.lazy(() => import("pages/source/SelectSourcePage"));
const SourceItemPage = React.lazy(() => import("pages/source/SourceItemPage"));
const SourceConnectionsPage = React.lazy(() => import("pages/source/SourceConnectionsPage"));
const SourceSettingsPage = React.lazy(() => import("pages/source/SourceSettingsPage"));
const CloudDefaultView = React.lazy(() => import("./views/CloudDefaultView"));
const CloudSettingsPage = React.lazy(() => import("./views/settings/CloudSettingsPage"));

const MainRoutes: React.FC = () => {
  const workspace = useCurrentWorkspace();

  const analyticsContext = useMemo(
    () => ({
      workspace_id: workspace.workspaceId,
      customer_id: workspace.customerId,
    }),
    [workspace]
  );
  useAnalyticsRegisterValues(analyticsContext);

  return (
    <ApiErrorBoundary>
      <Routes>
        <Route path={RoutePaths.Destination}>
          <Route index element={<AllDestinationsPage />} />
          <Route path={DestinationPaths.SelectDestinationNew} element={<SelectDestinationPage />} />
          <Route path={DestinationPaths.DestinationNew} element={<CreateDestinationPage />} />
          <Route path={DestinationPaths.Root} element={<DestinationItemPage />}>
            <Route index element={<DestinationSettingsPage />} />
            <Route path={DestinationPaths.Connections} element={<DestinationConnectionsPage />} />
          </Route>
        </Route>
        <Route path={RoutePaths.Source}>
          <Route index element={<AllSourcesPage />} />
          <Route path={SourcePaths.SelectSourceNew} element={<SelectSourcePage />} />
          <Route path={SourcePaths.SourceNew} element={<CreateSourcePage />} />
          <Route path={SourcePaths.Root} element={<SourceItemPage />}>
            <Route index element={<SourceSettingsPage />} />
            <Route path={SourcePaths.Connections} element={<SourceConnectionsPage />} />
          </Route>
        </Route>
        <Route path={`${RoutePaths.Connections}/*`} element={<ConnectionsRoutes />} />
        <Route path={`${RoutePaths.Settings}/*`} element={<CloudSettingsPage />} />
        <Route path={CloudRoutes.Billing} element={<BillingPage />} />
        <Route path={CloudRoutes.UpcomingFeatures} element={<UpcomingFeaturesPage />} />
        <Route path={`${RoutePaths.ConnectorBuilder}/*`} element={<ConnectorBuilderRoutes />} />
        <Route path="*" element={<Navigate to={RoutePaths.Connections} replace />} />
      </Routes>
    </ApiErrorBoundary>
  );
};

const CloudMainViewRoutes = () => {
  const { loginRedirect } = useQuery<{ loginRedirect: string }>();

  if (loginRedirect) {
    return <Navigate to={loginRedirect} replace />;
  }

  return (
    <Routes>
      <Route path={RoutePaths.SpeakeasyRedirect} element={<SpeakeasyRedirectPage />} />
      <Route path={RoutePaths.Workspaces} element={<CloudWorkspacesPage />} />
      <Route path={CloudRoutes.AuthFlow} element={<CompleteOauthRequest />} />
      <Route
        path={`${RoutePaths.Workspaces}/:workspaceId/*`}
        element={
          <CloudWorkspaceDataPrefetcher>
            <CloudMainView>
              <MainRoutes />
            </CloudMainView>
          </CloudWorkspaceDataPrefetcher>
        }
      />
      <Route path="*" element={<CloudDefaultView />} />
    </Routes>
  );
};

const CloudWorkspaceDataPrefetcher: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  usePrefetchCloudWorkspaceData();
  return <>{children}</>;
};

export const Routing: React.FC = () => {
  const { user, inited, providers, loggedOut, requirePasswordReset } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();
  const { pathname: originalPathname, search, hash } = useLocation();

  const loginRedirectSearchParam = `${createSearchParams({
    loginRedirect: `${originalPathname}${search}${hash}`,
  })}`;

  const loginRedirectTo =
    loggedOut && (originalPathname === "/" || originalPathname.includes("/settings/account"))
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
        ? { providers, email: user.email, isCorporate: isCorporateEmail(user.email), currentWorkspaceId: workspaceId }
        : {},
    [providers, user, workspaceId]
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
          {/*
            The firebase callback action route is available no matter wheter a user is logged in or not, since
            the verify email action need to work in both cases.
          */}
          <Route path={CloudRoutes.FirebaseAction} element={<FirebaseActionRoute />} />
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
                        {requirePasswordReset && (
                          <Route
                            path={CloudRoutes.ResetPassword}
                            element={<ResetPasswordPage requirePasswordReset={requirePasswordReset} />}
                          />
                        )}
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
