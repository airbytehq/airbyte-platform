import { FormattedMessage } from "react-intl";

import { Link } from "components/ui/Link";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

export const BackToDefineSourceButton = () => {
  const createLink = useCurrentWorkspaceLink();

  return (
    <Link to={createLink(`/${RoutePaths.Connections}/${ConnectionRoutePaths.ConnectionNew}`)} variant="button">
      <FormattedMessage id="connectionForm.backToDefineSource" />
    </Link>
  );
};
