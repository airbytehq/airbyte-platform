import React, { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import { RoutePaths } from "pages/routePaths";

/**
 * no dbt integration set up message
 */
export const NoDbtIntegrationMsg: React.FC = () => {
  const workspaceId = useCurrentWorkspaceId();
  const dbtSettingsPath = `/${RoutePaths.Workspaces}/${workspaceId}/${RoutePaths.Settings}/dbt-cloud`;

  return (
    <Text color="grey300">
      <FormattedMessage
        id="connection.dbtCloudJobs.noIntegration"
        values={{
          lnk: (linkText: ReactNode) => <Link to={dbtSettingsPath}>{linkText}</Link>,
        }}
      />
    </Text>
  );
};
