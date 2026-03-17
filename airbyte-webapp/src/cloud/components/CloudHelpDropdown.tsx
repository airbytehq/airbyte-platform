import { FormattedMessage, useIntl } from "react-intl";

import { DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { Icon } from "components/ui/Icon";

import { HelpDropdownProps } from "area/layout/SideBar/components/HelpDropdown";
import { NavDropdown } from "area/layout/SideBar/components/NavDropdown";
import { useSupportAgentService } from "cloud/services/supportAgent";
import { useFeature, FeatureItem } from "core/services/features";
import { links } from "core/utils/links";

export const CloudHelpDropdown: React.FC<HelpDropdownProps> = ({ className, hideLabel, placement }) => {
  const { formatMessage } = useIntl();
  const supportEnabled = useFeature(FeatureItem.SupportAgentBot);
  const { openSupportBot } = useSupportAgentService();

  const handleChatUs = (data: DropdownMenuOptionType) => {
    if (data.value === "inApp") {
      openSupportBot();
    }
  };

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
        ...(supportEnabled
          ? [
              {
                as: "button" as const,
                value: "inApp",
                icon: <Icon type="chat" />,
                displayName: formatMessage({ id: "sidebar.inAppHelpCenter" }),
              },
            ]
          : []),
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
      ]}
      onChange={handleChatUs}
      label={hideLabel ? undefined : <FormattedMessage id="sidebar.help" />}
      icon="question"
      placement={placement}
    />
  );
};
