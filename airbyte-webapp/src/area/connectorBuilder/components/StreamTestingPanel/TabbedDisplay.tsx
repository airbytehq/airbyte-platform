import { Tab, TabGroup, TabList, TabPanel, TabPanels } from "@headlessui/react";
import classNames from "classnames";
import { useEffect, useState } from "react";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./TabbedDisplay.module.scss";

interface TabbedDisplayProps {
  className?: string;
  tabs: TabData[];
  defaultTabIndex?: number;
}

export interface TabData {
  key: string;
  title: React.ReactNode;
  content: React.ReactNode;
  "data-testid"?: string;
}

export const TabbedDisplay: React.FC<TabbedDisplayProps> = ({ className, tabs, defaultTabIndex = 0 }) => {
  const [selectedIndex, setSelectedIndex] = useState(defaultTabIndex);
  useEffect(() => {
    if (selectedIndex >= tabs.length) {
      setSelectedIndex(defaultTabIndex);
    }
  }, [defaultTabIndex, selectedIndex, tabs.length]);

  return (
    <TabGroup className={styles.tabGroup} selectedIndex={selectedIndex} onChange={setSelectedIndex}>
      <FlexContainer className={classNames(className, styles.container)} direction="column">
        <TabList className={styles.tabList}>
          {tabs.map((tab) => (
            <Tab className={styles.tab} key={tab.key} data-testid={tab["data-testid"]}>
              {({ selected }) => (
                <Text
                  className={classNames(styles.tabTitle, { [styles.selected]: selected })}
                  size="xs"
                  align="center"
                  as="div"
                >
                  {tab.title}
                </Text>
              )}
            </Tab>
          ))}
        </TabList>
        <TabPanels className={styles.tabPanelContainer}>
          {tabs.map((tab) => (
            <TabPanel className={styles.tabPanel} key={tab.key}>
              {tab.content}
            </TabPanel>
          ))}
        </TabPanels>
      </FlexContainer>
    </TabGroup>
  );
};
