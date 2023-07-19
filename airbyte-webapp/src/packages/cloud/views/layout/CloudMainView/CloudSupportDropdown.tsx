import { faQuestionCircle } from "@fortawesome/free-regular-svg-icons";
import { faUpRightFromSquare } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage, useIntl } from "react-intl";

import { DropdownMenuOptionType } from "components/ui/DropdownMenu";

import { links } from "core/utils/links";
import { useZendesk } from "packages/cloud/services/thirdParty/zendesk";
import ChatIcon from "views/layout/SideBar/components/ChatIcon";
import { NavDropdown } from "views/layout/SideBar/components/NavDropdown";

export const CloudSupportDropdown: React.FC = () => {
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
      ]}
      onChange={handleChatUs}
      label={<FormattedMessage id="sidebar.support" />}
      icon={<FontAwesomeIcon icon={faQuestionCircle} style={{ height: "22px" }} />}
    />
  );
};
