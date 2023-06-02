import React, { Suspense, useMemo } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";

import { ApiErrorBoundary } from "components/common/ApiErrorBoundary";
import LoadingPage from "components/LoadingPage";

import { useAnalyticsIdentifyUser, useAnalyticsRegisterValues } from "core/services/analytics/useAnalyticsService";
import { useBuildUpdateCheck } from "hooks/services/useBuildUpdateCheck";
import { useQuery } from "hooks/useQuery";
import { useAuthService } from "packages/cloud/services/auth/AuthService";
import ConnectorBuilderRoutes from "pages/connectorBuilder/ConnectorBuilderRoutes";
import { RoutePaths, DestinationPaths, SourcePaths } from "pages/routePaths";
import {
  useCurrentWorkspace,
  WorkspaceServiceProvider,
  usePrefetchCloudWorkspaceData,
  useCurrentWorkspaceId,
} from "services/workspaces/WorkspacesService";
import { CompleteOauthRequest } from "views/CompleteOauthRequest";

import { CloudRoutes } from "./cloudRoutePaths";
import { LDExperimentServiceProvider } from "./services/thirdParty/launchdarkly";
import { FirebaseActionRoute } from "./views/FirebaseActionRoute";

const LoginPage = React.lazy(() => import("./views/auth/LoginPage"));
const ResetPasswordPage = React.lazy(() => import("./views/auth/ResetPasswordPage"));
const SignupPage = React.lazy(() => import("./views/auth/SignupPage"));
const CloudMainView = React.lazy(() => import("packages/cloud/views/layout/CloudMainView"));
const WorkspacesPage = React.lazy(() => import("packages/cloud/views/workspaces"));
const AuthLayout = React.lazy(() => import("packages/cloud/views/auth"));
const BillingPage = React.lazy(() => import("packages/cloud/views/billing"));
const UpcomingFeaturesPage = React.lazy(() => import("packages/cloud/views/UpcomingFeaturesPage"));
const SpeakeasyRedirectPage = React.lazy(() => import("pages/SpeakeasyRedirectPage"));

const ConnectionsRoutes = React.lazy(() => import("pages/connections/ConnectionsRoutes"));

const AllDestinationsPage = React.lazy(() => import("pages/destination/AllDestinationsPage"));
const CreateDestinationPage = React.lazy(() => import("pages/destination/CreateDestinationPage"));
const SelectDestinationPage = React.lazy(() => import("pages/destination/SelectDestinationPage"));
const DestinationItemPage = React.lazy(() => import("pages/destination/DestinationItemPage"));
const DestinationOverviewPage = React.lazy(() => import("pages/destination/DestinationOverviewPage"));
const DestinationSettingsPage = React.lazy(() => import("pages/destination/DestinationSettingsPage"));

const AllSourcesPage = React.lazy(() => import("pages/source/AllSourcesPage"));
const CreateSourcePage = React.lazy(() => import("pages/source/CreateSourcePage"));
const SelectSourcePage = React.lazy(() => import("pages/source/SelectSourcePage"));
const SourceItemPage = React.lazy(() => import("pages/source/SourceItemPage"));
const SourceOverviewPage = React.lazy(() => import("pages/source/SourceOverviewPage"));
const SourceSettingsPage = React.lazy(() => import("pages/source/SourceSettingsPage"));

const CloudSettingsPage = React.lazy(() => import("./views/settings/CloudSettingsPage"));
const DefaultView = React.lazy(() => import("./views/DefaultView"));

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
            <Route index element={<DestinationOverviewPage />} />
            <Route path={DestinationPaths.Settings} element={<DestinationSettingsPage />} />
          </Route>
        </Route>
        <Route path={RoutePaths.Source}>
          <Route index element={<AllSourcesPage />} />
          <Route path={SourcePaths.SelectSourceNew} element={<SelectSourcePage />} />
          <Route path={SourcePaths.SourceNew} element={<CreateSourcePage />} />
          <Route path={SourcePaths.Root} element={<SourceItemPage />}>
            <Route index element={<SourceOverviewPage />} />
            <Route path={SourcePaths.Settings} element={<SourceSettingsPage />} />
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
  const query = useQuery<{ from: string }>();

  return (
    <Routes>
      <Route path={RoutePaths.SpeakeasyRedirect} element={<SpeakeasyRedirectPage />} />
      {[CloudRoutes.Login, CloudRoutes.Signup, CloudRoutes.FirebaseAction].map((r) => (
        <Route key={r} path={`${r}/*`} element={query.from ? <Navigate to={query.from} replace /> : <DefaultView />} />
      ))}
      <Route path={RoutePaths.Workspaces} element={<WorkspacesPage />} />
      <Route path={CloudRoutes.AuthFlow} element={<CompleteOauthRequest />} />
      <Route
        path={`${RoutePaths.Workspaces}/:workspaceId/*`}
        element={
          <CloudMainView>
            <MainRoutes />
          </CloudMainView>
        }
      />
      <Route path="*" element={<DefaultView />} />
    </Routes>
  );
};

const CloudWorkspaceDataPrefetcher = () => {
  usePrefetchCloudWorkspaceData();
  return null;
};

export const Routing: React.FC = () => {
  const { user, inited, providers, hasCorporateEmail, loggedOut } = useAuthService();
  const workspaceId = useCurrentWorkspaceId();
  const { pathname } = useLocation();

  useBuildUpdateCheck();

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
    () => (user ? { providers, email: user.email, isCorporate: hasCorporateEmail() } : {}),
    [hasCorporateEmail, providers, user]
  );

  useAnalyticsRegisterValues(analyticsContext);
  useAnalyticsIdentifyUser(user?.userId, userTraits);

  if (!inited) {
    return <LoadingPage />;
  }

  return (
    <WorkspaceServiceProvider>
      <LDExperimentServiceProvider>
        {workspaceId && user && <CloudWorkspaceDataPrefetcher />}
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
                          <Route path={CloudRoutes.Login} element={<LoginPage />} />
                          <Route path={CloudRoutes.Signup} element={<SignupPage />} />
                          <Route path={CloudRoutes.ResetPassword} element={<ResetPasswordPage />} />
                          {/* In case a not logged in user tries to access anything else navigate them to login */}
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
    </WorkspaceServiceProvider>
  );
};
