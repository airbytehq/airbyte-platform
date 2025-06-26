import { Placement } from "@floating-ui/react-dom";
import { FormattedMessage, useIntl } from "react-intl";

import { Box } from "components/ui/Box";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { links } from "core/utils/links";

import styles from "./HelpDropdown.module.scss";
import { NavDropdown } from "./NavDropdown";

export interface HelpDropdownProps {
  className?: string;
  hideLabel?: boolean;
  placement?: Placement;
}

export const HelpDropdown: React.FC<HelpDropdownProps> = ({ className, hideLabel, placement }) => {
  const { formatMessage } = useIntl();
  return (
    <NavDropdown
      className={className}
      options={[
        {
          as: "a",
          href: links.docsLink,
          icon: <Icon type="docs" />,
          displayName: formatMessage({ id: "sidebar.documentation" }),
        },
        {
          as: "a",
          href: links.gettingSupport,
          icon: <Icon type="chat" />,
          displayName: formatMessage({ id: "sidebar.gettingSupport" }),
        },
        {
          as: "a",
          href: links.slackLink,
          icon: <Icon type="slack" />,
          displayName: formatMessage({ id: "sidebar.joinSlack" }),
        },
        {
          as: "separator",
        },
        {
          as: "div",
          children: (
            <Box py="sm" px="lg">
              <Text as="div" size="sm" align="right" color="grey500">
                <FlexContainer justifyContent="space-between">
                  <FlexItem>{process.env.REACT_APP_VERSION}</FlexItem>
                  <ExternalLink href={links.updateLink} className={styles.updateLink}>
                    {formatMessage({ id: "sidebar.howToUpdate" })}
                  </ExternalLink>
                </FlexContainer>
              </Text>
            </Box>
          ),
        },
      ]}
      label={hideLabel ? undefined : <FormattedMessage id="sidebar.help" />}
      icon="question"
      placement={placement}
    />
  );
};
