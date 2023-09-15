import { Navigate, useParams } from "react-router-dom";

import { FlexContainer } from "components/ui/Flex";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { CloudRoutes } from "packages/cloud/cloudRoutePaths";

// A bookmarkable route that redirects to the SSO login page with the provided company identifier
export const SSOBookmarkPage = () => {
  const { companyIdentifier } = useParams();

  if (!companyIdentifier) {
    return <Navigate to={CloudRoutes.Login} />;
  }

  // TODO: configure this realm in react-oidc-context and redirect to keycloak. For now, display a harmless error message in case someone stumbles across this.
  return (
    <FlexContainer direction="column">
      <Text>
        Company identifier <b>{companyIdentifier}</b> not found.
      </Text>
      <Text>
        <Link to={CloudRoutes.Sso}>Back to SSO login</Link>
      </Text>
    </FlexContainer>
  );
};
