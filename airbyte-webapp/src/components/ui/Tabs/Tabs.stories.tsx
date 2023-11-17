import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { Box } from "components/ui/Box";

import { Tabs, ButtonTab } from ".";

export default {
  title: "UI/Tabs",
  component: Tabs,
  argTypes: {},
} as Meta<typeof Tabs>;

const Template: StoryFn<typeof Tabs> = () => {
  const data = [
    {
      id: "status",
      name: "Status",
    },
    {
      id: "replication",
      name: "Replication",
    },
    {
      id: "transformation",
      name: "Transformation",
    },
  ];

  const [selected, setSelected] = useState(data[0].id);

  return (
    <Box py="2xl">
      <Tabs>
        {data.map((tabItem) => {
          return (
            <ButtonTab
              id={tabItem.id}
              key={tabItem.id}
              name={tabItem.name}
              isActive={selected === tabItem.id}
              onSelect={(val) => {
                setSelected(val);
              }}
            />
          );
        })}
      </Tabs>
    </Box>
  );
};

export const Primary = Template.bind({});
