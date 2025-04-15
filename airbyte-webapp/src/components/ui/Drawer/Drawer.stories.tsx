import { StoryFn, StoryObj } from "@storybook/react";
import { useState } from "react";

import { CatalogDiffModal } from "components/connection/CatalogDiffModal";
import { mockCatalogDiff } from "test-utils/mock-data/mockCatalogDiff";

import { Drawer } from "./Drawer";
import { Box } from "../Box";
import { Button } from "../Button";
import { Heading } from "../Heading";

export default {
  title: "ui/Drawer",
  component: Drawer,
  argTypes: {},
} as StoryObj<typeof Drawer>;

const DefaultTemplate: StoryFn<typeof Drawer> = () => {
  const [isOpen, setIsOpen] = useState(false);
  return (
    <div style={{ backgroundColor: "light-blue" }}>
      <Button onClick={() => setIsOpen(true)}>Open Drawer</Button>

      <Drawer
        title={
          <Box px="lg">
            <Heading as="h2">I'm a drawer</Heading>
          </Box>
        }
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
      >
        <Box px="lg">Hello, World!</Box>
      </Drawer>
    </div>
  );
};

const CatalogDiffDrawerTemplate: StoryFn<typeof Drawer> = () => {
  const [isOpen, setIsOpen] = useState(false);
  return (
    <div style={{ backgroundColor: "light-blue" }}>
      <Button onClick={() => setIsOpen(true)}>Open Drawer</Button>

      <Drawer
        title={
          <Box px="lg">
            <Heading as="h2">Catalog Diff</Heading>
          </Box>
        }
        isOpen={isOpen}
        onClose={() => setIsOpen(false)}
      >
        <Box p="lg">
          <CatalogDiffModal catalogDiff={mockCatalogDiff} />
        </Box>
      </Drawer>
    </div>
  );
};

export const Default = DefaultTemplate.bind({});
export const CatalogDiffDrawer = CatalogDiffDrawerTemplate.bind({});
