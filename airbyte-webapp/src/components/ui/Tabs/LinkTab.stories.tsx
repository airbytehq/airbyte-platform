import { Meta, StoryFn } from "@storybook/react";
import { Routes, Route, useLocation } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { Tabs, LinkTab } from ".";

export default {
  title: "UI/LinkTab",
  component: LinkTab,
} as Meta<typeof LinkTab>;

/**
 * LinkTab is a navigation tab component that uses React Router's Link for client-side routing.
 * Unlike ButtonTab which triggers a callback, LinkTab navigates to a different route.
 *
 * Use Cases:
 * - Page navigation tabs (e.g., Connection Status, Replication, Transformation tabs)
 * - Section navigation within an application
 * - Any tabbed interface where each tab represents a different route
 *
 * Key Differences from ButtonTab:
 * - LinkTab uses `to` prop (route path) instead of `onSelect` callback
 * - Automatically renders as an anchor tag for better accessibility and SEO
 * - Supports disabled state which prevents navigation
 * - Works with React Router for declarative routing
 */

const LinkTabDemo = () => {
  const location = useLocation();
  const currentPath = location.pathname;

  const tabs = [
    {
      id: "status",
      name: "Status",
      to: "/status",
    },
    {
      id: "replication",
      name: "Replication",
      to: "/replication",
    },
    {
      id: "transformation",
      name: "Transformation",
      to: "/transformation",
    },
    {
      id: "settings",
      name: "Settings",
      to: "/settings",
    },
  ];

  return (
    <FlexContainer direction="column" gap="xl">
      <Box py="2xl">
        <Tabs>
          {tabs.map((tab) => (
            <LinkTab key={tab.id} id={tab.id} name={tab.name} to={tab.to} isActive={currentPath === tab.to} />
          ))}
        </Tabs>
      </Box>

      <Box p="lg">
        <Routes>
          <Route path="/status" element={<Text>Status Page Content</Text>} />
          <Route path="/replication" element={<Text>Replication Page Content</Text>} />
          <Route path="/transformation" element={<Text>Transformation Page Content</Text>} />
          <Route path="/settings" element={<Text>Settings Page Content</Text>} />
        </Routes>
      </Box>
    </FlexContainer>
  );
};

const Template: StoryFn<typeof LinkTab> = () => <LinkTabDemo />;

export const Default = Template.bind({});
Default.parameters = {
  docs: {
    description: {
      story: "Basic LinkTab navigation with multiple tabs. Click tabs to navigate between routes.",
    },
  },
};

const DisabledTabDemo = () => {
  const location = useLocation();
  const currentPath = location.pathname;

  const tabs = [
    {
      id: "available",
      name: "Available",
      to: "/available",
      disabled: false,
    },
    {
      id: "disabled",
      name: "Disabled Tab",
      to: "/disabled",
      disabled: true,
    },
    {
      id: "another",
      name: "Another Available",
      to: "/another",
      disabled: false,
    },
  ];

  return (
    <FlexContainer direction="column" gap="xl">
      <Box py="2xl">
        <Tabs>
          {tabs.map((tab) => (
            <LinkTab
              key={tab.id}
              id={tab.id}
              name={tab.name}
              to={tab.to}
              isActive={currentPath === tab.to}
              disabled={tab.disabled}
            />
          ))}
        </Tabs>
      </Box>

      <Box p="lg">
        <Routes>
          <Route path="/available" element={<Text>Available Tab Content</Text>} />
          <Route path="/disabled" element={<Text>This should not be accessible</Text>} />
          <Route path="/another" element={<Text>Another Available Tab Content</Text>} />
        </Routes>
      </Box>
    </FlexContainer>
  );
};

const DisabledTemplate: StoryFn<typeof LinkTab> = () => <DisabledTabDemo />;

export const WithDisabledTab = DisabledTemplate.bind({});
WithDisabledTab.parameters = {
  docs: {
    description: {
      story:
        "LinkTab supports disabled state. Disabled tabs cannot be clicked and are visually distinguished. This is useful when certain sections are temporarily unavailable or locked behind permissions.",
    },
  },
};

const ComplexContentDemo = () => {
  const location = useLocation();
  const currentPath = location.pathname;

  const tabs = [
    {
      id: "overview",
      name: (
        <FlexContainer gap="sm" as="span">
          <span>Overview</span>
        </FlexContainer>
      ),
      to: "/overview",
    },
    {
      id: "details",
      name: (
        <FlexContainer gap="sm" as="span">
          <span>Details</span>
          <span style={{ fontSize: "12px", opacity: 0.7 }}>(3)</span>
        </FlexContainer>
      ),
      to: "/details",
    },
  ];

  return (
    <FlexContainer direction="column" gap="xl">
      <Box py="2xl">
        <Tabs>
          {tabs.map((tab) => (
            <LinkTab key={tab.id} id={tab.id} name={tab.name} to={tab.to} isActive={currentPath === tab.to} />
          ))}
        </Tabs>
      </Box>

      <Box p="lg">
        <Routes>
          <Route path="/overview" element={<Text>Overview Content</Text>} />
          <Route path="/details" element={<Text>Details Content</Text>} />
        </Routes>
      </Box>
    </FlexContainer>
  );
};

const ComplexTemplate: StoryFn<typeof LinkTab> = () => <ComplexContentDemo />;

export const WithComplexContent = ComplexTemplate.bind({});
WithComplexContent.parameters = {
  docs: {
    description: {
      story:
        "LinkTab `name` prop accepts React nodes, allowing for complex tab labels with icons, badges, or custom formatting. This is used in production for showing schema change indicators or notification counts.",
    },
  },
};
