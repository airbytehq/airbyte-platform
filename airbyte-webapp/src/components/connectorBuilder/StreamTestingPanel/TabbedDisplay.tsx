import { Tab } from "@headlessui/react";
import classNames from "classnames";
import { useState } from "react";

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

  return (
    <FlexContainer className={classNames(className, styles.container)} direction="column">
      <Tab.Group selectedIndex={selectedIndex} onChange={setSelectedIndex}>
        <Tab.List className={styles.tabList}>
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
        </Tab.List>
        <Tab.Panels className={styles.tabPanelContainer}>
          {tabs.map((tab) => (
            <Tab.Panel className={styles.tabPanel} key={tab.key}>
              {tab.content}
            </Tab.Panel>
          ))}
        </Tab.Panels>
      </Tab.Group>
    </FlexContainer>
  );
};
