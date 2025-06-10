import { useEffect } from "react";
import { Navigate, useParams, useSearchParams } from "react-router-dom";

import { useListOrganizationsByUser, useListWorkspacesInfinite } from "core/api";
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

  // Set workspaceId in search params if missing, after data is loaded
  useEffect(() => {
    if (!searchParams.has(WORKSPACE_ID_PARAM) && workspacesData && organizationId) {
      for (const page of workspacesData.pages ?? []) {
        for (const workspace of page.data.workspaces) {
          if (workspace.organizationId === organizationId) {
            const newSearchParams = new URLSearchParams(searchParams.toString());
            newSearchParams.set(WORKSPACE_ID_PARAM, workspace.workspaceId);
            setSearchParams(newSearchParams);
            break;
          }
        }
      }
    }
  }, [searchParams, workspacesData, organizationId, setSearchParams]);
  return <EmbeddedOnboardingPageLayout />;
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
      <div>
        <EmbeddedUpsell />
      </div>
    );
  }

  return (
    <Navigate
      to={`/${RoutePaths.Organization}/${organizations[0]?.organizationId}/${RoutePaths.EmbeddedOnboarding}`}
      replace
    />
  );
};
