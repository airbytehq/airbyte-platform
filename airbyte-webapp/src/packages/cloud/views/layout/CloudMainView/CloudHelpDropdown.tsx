import { FormattedMessage, useIntl } from "react-intl";

import { DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { links } from "core/utils/links";
import { CloudRoutes } from "packages/cloud/cloudRoutePaths";
import { useZendesk } from "packages/cloud/services/thirdParty/zendesk";
import { HelpDropdownProps } from "views/layout/SideBar/components/HelpDropdown";
import { NavDropdown } from "views/layout/SideBar/components/NavDropdown";

export const CloudHelpDropdown: React.FC<HelpDropdownProps> = ({ className, hideLabel, placement }) => {
  const { formatMessage } = useIntl();
  const { openZendesk } = useZendesk();
  const handleChatUs = (data: DropdownMenuOptionType) => data.value === "inApp" && openZendesk();
  const createLink = useCurrentWorkspaceLink();

  return (
    <NavDropdown
      className={className}
      options={[
        {
          as: "a",
          href: links.supportPortal,
          icon: <Icon type="share" />,
          displayName: formatMessage({ id: "sidebar.supportPortal" }),
        },
        {
          as: "button",
          value: "inApp",
          icon: <Icon type="chat" />,
          displayName: formatMessage({ id: "sidebar.inAppHelpCenter" }),
        },
        {
          as: "separator",
        },
        {
          as: "a",
          href: links.docsLink,
          icon: <Icon type="docs" />,
          displayName: formatMessage({ id: "sidebar.documentation" }),
        },
        {
          as: "a",
          href: links.statusLink,
          icon: <Icon type="pulse" />,
          displayName: formatMessage({ id: "sidebar.status" }),
        },
        {
          as: "a",
          internal: true,
          href: createLink(`/${CloudRoutes.UpcomingFeatures}`),
          icon: <Icon type="calendarCheck" />,
          displayName: formatMessage({ id: "sidebar.upcomingFeatures" }),
        },
      ]}
      onChange={handleChatUs}
      label={hideLabel ? undefined : <FormattedMessage id="sidebar.help" />}
      icon="question"
      placement={placement}
    />
  );
};
