import { faCalendarCheck, faQuestionCircle } from "@fortawesome/free-regular-svg-icons";
import { faUpRightFromSquare } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage, useIntl } from "react-intl";

import { DocsIcon } from "components/icons/DocsIcon";
import { DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { links } from "core/utils/links";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useZendesk } from "packages/cloud/services/thirdParty/zendesk";
import ChatIcon from "views/layout/SideBar/components/ChatIcon";
import { NavDropdown } from "views/layout/SideBar/components/NavDropdown";
import StatusIcon from "views/layout/SideBar/components/StatusIcon";

export const CloudHelpDropdown: React.FC = () => {
  const { formatMessage } = useIntl();
  const { openZendesk } = useZendesk();
  const handleChatUs = (data: DropdownMenuOptionType) => data.value === "inApp" && openZendesk();

  return (
    <NavDropdown
      options={[
        {
          as: "a",
          href: links.supportPortal,
          icon: <FontAwesomeIcon icon={faUpRightFromSquare} size="sm" />,
          displayName: formatMessage({ id: "sidebar.supportPortal" }),
        },
        {
          as: "button",
          value: "inApp",
          icon: <ChatIcon />,
          displayName: formatMessage({ id: "sidebar.inAppHelpCenter" }),
        },
        {
          as: "separator",
        },
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
      onChange={handleChatUs}
      label={<FormattedMessage id="sidebar.help" />}
      icon={<FontAwesomeIcon icon={faQuestionCircle} style={{ height: "22px" }} />}
    />
  );
};
