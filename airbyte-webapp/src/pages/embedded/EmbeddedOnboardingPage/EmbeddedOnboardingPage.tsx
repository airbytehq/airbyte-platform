import { useEffect, useMemo } from "react";
import { Navigate, useParams, useSearchParams } from "react-router-dom";

import { useListOrganizationsByUser, useListWorkspacesInfinite } from "core/api";
import { DefaultErrorBoundary } from "core/errors";
import { useCurrentUser } from "core/services/auth";
import { RoutePaths } from "pages/routePaths";

import { EmbeddedOnboardingPageLayout } from "./EmbeddedOnboardingPageLayout";
import { EmbeddedUpsell } from "./EmbeddedUpsell";
import { WORKSPACE_ID_PARAM } from "../EmbeddedSourceCreatePage/hooks/useEmbeddedSourceParams";

/**
 * todo: the double redirect here causes a flicker on load.
 * leaving this as is for now, but will revisit prior to release once we have the
 * organization id endpoint from sonar.
 *
 * https://github.com/airbytehq/airbyte-internal-issues/issues/13194
 *
 */
export const EmbeddedOnboardingPage: React.FC = () => {
  const { data: workspacesData } = useListWorkspacesInfinite(1000, "", true);
  const [searchParams, setSearchParams] = useSearchParams();
  const organizationId = useParams<{ organizationId: string }>().organizationId;

  // Only compute matchingWorkspaceId if workspaceId param is missing
  const matchingWorkspaceId = useMemo(() => {
    if (searchParams.has(WORKSPACE_ID_PARAM) || !workspacesData || !organizationId) {
      return undefined;
    }
    for (const page of workspacesData.pages ?? []) {
      for (const workspace of page.data.workspaces) {
        if (workspace.organizationId === organizationId) {
          return workspace.workspaceId;
        }
      }
    }
    return undefined;
  }, [searchParams, workspacesData, organizationId]);

  // Set workspaceId in search params if missing, after data is loaded
  useEffect(() => {
    if (!searchParams.has(WORKSPACE_ID_PARAM) && matchingWorkspaceId) {
      const newSearchParams = new URLSearchParams(searchParams.toString());
      newSearchParams.set(WORKSPACE_ID_PARAM, matchingWorkspaceId);
      setSearchParams(newSearchParams);
    }
  }, [searchParams, matchingWorkspaceId, setSearchParams]);
  return (
    <DefaultErrorBoundary>
      <EmbeddedOnboardingPageLayout />
    </DefaultErrorBoundary>
  );
};

export const EmbeddedOnboardingRedirect: React.FC = () => {
  /**
   * Note: This is set of flag + org id checks a hacky start for how to fetch the organization id and check
   * onboarding status. This will be replaced with an endpoint that does the smart parts for us prior to
   * shipping this feature!
   * https://github.com/airbytehq/airbyte-internal-issues/issues/13194
   *
   * I repeat: this is for development + testing and will NOT work reliably in production :)
   */

  const { userId } = useCurrentUser();
  const { organizations } = useListOrganizationsByUser({ userId });

  if (organizations.length === 0) {
    // If no organizations are found, redirect to the workspaces page
    return (
      <DefaultErrorBoundary>
        <EmbeddedUpsell />
      </DefaultErrorBoundary>
    );
  }

  return (
    <Navigate
      to={`/${RoutePaths.Organization}/${organizations[0]?.organizationId}/${RoutePaths.EmbeddedOnboarding}`}
      replace
    />
  );
};
