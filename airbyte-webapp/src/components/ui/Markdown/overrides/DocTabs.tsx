import { Children, isValidElement, ReactElement, useState } from "react";

import { Box } from "components/ui/Box";
import { Tabs, ButtonTab } from "components/ui/Tabs";

interface DocTabsProps {
  children: Array<ReactElement<TabItemProps>>;
}

interface TabItemProps {
  value: string;
  label: string;
  default?: boolean;
  children: ReactElement[];
}

export const DocTabs = ({ children }: DocTabsProps) => {
  const childNodes = Children.toArray(children).filter(isValidElement<TabItemProps>);
  // If one of the child <TabItem>s has a `default` attribute, use that as the default tab
  // (if there's more than one <TabItem default>, just use the first). Otherwise, the
  // first tab is the default.
  const defaultTab = childNodes.find((child) => child.props.default)?.props.value || childNodes[0]?.props.value;
  const [selectedTab, setSelectedTab] = useState(defaultTab);
  const tabButtons = childNodes.map((childNode) => {
    const { value, label } = childNode.props;
    return (
      <ButtonTab
        id={value}
        key={value}
        name={label}
        isActive={selectedTab === value}
        onSelect={(value) => setSelectedTab(value)}
      />
    );
  });
  const tabBoxContent = childNodes.map((childNode: ReactElement<TabItemProps>) => {
    const { value, children } = childNode.props;
    return selectedTab === value && <Box key={value}>{children}</Box>;
  });

  return (
    <Box pt="sm">
      <Tabs>{tabButtons}</Tabs>
      <Box pt="sm">{tabBoxContent}</Box>
    </Box>
  );
};
