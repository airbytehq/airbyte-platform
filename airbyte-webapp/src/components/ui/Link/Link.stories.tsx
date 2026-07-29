import { StoryFn, Meta } from "@storybook/react";

import { Text } from "components/ui/Text";

import { Link } from "./index";

export default {
  title: "Ui/Link",
  component: Link,
} as Meta<typeof Link>;

const Template: StoryFn<typeof Link> = (args) => <Link {...args} />;

export const Primary = Template.bind({});
Primary.args = {
  children: "Click me!",
  to: "/",
};

export const WithinText = () => (
  <>
    <Text size="lg">
      Large sized text <Link to="/">Learn more</Link>.
    </Text>
    <Text size="sm">
      Small sized text <Link to="/">Learn more</Link>.
    </Text>
  </>
);
