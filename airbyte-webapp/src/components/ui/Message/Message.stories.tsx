import { ComponentStory, ComponentMeta } from "@storybook/react";

import { Message } from "./Message";

export default {
  title: "UI/Message",
  component: Message,
  argTypes: {
    text: { type: { name: "string", required: false } },
    type: { type: { name: "string", required: false } },
    onAction: { table: { disable: true } },
    actionBtnText: { type: { name: "string", required: false } },
    onClose: { table: { disable: true } },
  },
} as ComponentMeta<typeof Message>;

const Template: ComponentStory<typeof Message> = (args) => <Message {...args} />;

export const Basic = Template.bind({});
Basic.args = {
  text: "This is an info message",
};

export const WithText = Template.bind({});
WithText.args = {
  text: "Use this message to inform the user",
};

export const WithLongText = Template.bind({});
WithLongText.args = {
  text: "Here is some information for the user, not scary, just an FYI",
};

export const WithCloseButton = Template.bind({});
WithCloseButton.args = {
  text: "This is an info message with a close button",
  onClose: () => {
    console.log("Closed!");
  },
};

export const WithActionButton = Template.bind({});
WithActionButton.args = {
  text: "This is an info message with an action button",
  onAction: () => console.log("Action btn clicked!"),
  actionBtnText: "Click me!",
};

export const WithActionAndCloseButton = Template.bind({});
WithActionAndCloseButton.args = {
  text: "This is an info message with an action button",
  onAction: () => console.log("Action btn clicked!"),
  actionBtnText: "Click me!",
  onClose: () => console.log("Closed!"),
};

export const WarningMessage = Template.bind({});
WarningMessage.args = {
  text: "This is a warning message. No breaking change yet, but it's coming!",
  onClose: () => console.log("Closed!"),
  type: "warning",
};

export const ErrorMessage = Template.bind({});
ErrorMessage.args = {
  text: "This is an error message. Something is broken!",
  onClose: () => console.log("Closed!"),
  type: "error",
};

export const SuccessMessage = Template.bind({});
SuccessMessage.args = {
  text: "This is a success message. All's well!",
  onClose: () => console.log("Closed!"),
  type: "success",
};

export const WithSecondaryText = Template.bind({});
WithSecondaryText.args = {
  text: "This is an info message with some secondary text.",
  secondaryText: "This is a secondary text",
  onClose: () => console.log("Closed!"),
  type: "info",
};
