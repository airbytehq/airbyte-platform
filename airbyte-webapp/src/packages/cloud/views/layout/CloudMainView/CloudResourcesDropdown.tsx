import { faCalendarCheck } from "@fortawesome/free-regular-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage, useIntl } from "react-intl";

import { DocsIcon } from "components/icons/DocsIcon";

import { links } from "core/utils/links";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { NavDropdown } from "views/layout/SideBar/components/NavDropdown";
import StatusIcon from "views/layout/SideBar/components/StatusIcon";

export const CloudResourcesDropdown: React.FC = () => {
  const { formatMessage } = useIntl();
  return (
    <NavDropdown
      options={[
        {
          as: "a",
          href: links.docsLink,
          icon: <DocsIcon />,
          displayName: formatMessage({ id: "sidebar.documentation" }),
        },
        {
          as: "a",
          href: links.statusLink,
          icon: <StatusIcon />,
          displayName: formatMessage({ id: "sidebar.status" }),
        },
        {
          as: "a",
          internal: true,
          href: CloudRoutes.UpcomingFeatures,
          icon: <FontAwesomeIcon icon={faCalendarCheck} />,
          displayName: formatMessage({ id: "sidebar.upcomingFeatures" }),
        },
      ]}
      label={<FormattedMessage id="sidebar.resources" />}
      icon={<DocsIcon />}
    />
  );
};
