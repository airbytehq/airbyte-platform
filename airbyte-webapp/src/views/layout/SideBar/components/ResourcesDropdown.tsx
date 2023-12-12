import { FormattedMessage, useIntl } from "react-intl";

import { Icon } from "components/ui/Icon";

import { links } from "core/utils/links";

import { NavDropdown } from "./NavDropdown";

export const ResourcesDropdown: React.FC = () => {
  const { formatMessage } = useIntl();
  return (
    <NavDropdown
      options={[
        {
          as: "a",
          href: links.docsLink,
          icon: <Icon type="docs" size="lg" />,
          displayName: formatMessage({ id: "sidebar.documentation" }),
        },
        {
          as: "a",
          href: links.slackLink,
          icon: <Icon type="slack" />,
          displayName: formatMessage({ id: "sidebar.joinSlack" }),
        },
        {
          as: "a",
          href: links.tutorialLink,
          icon: <Icon type="recipes" />,
          displayName: formatMessage({ id: "sidebar.recipes" }),
        },
      ]}
      label={<FormattedMessage id="sidebar.resources" />}
      icon={<Icon type="docs" />}
    />
  );
};
