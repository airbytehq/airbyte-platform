import { ComponentStory, ComponentMeta } from "@storybook/react";

import { Message, MessageType } from "./Message";

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
  text: "This is a basic card",
};

export const WithText = Template.bind({});
WithText.args = {
  text: "This is a card with a text",
};

export const WithLongText = Template.bind({});
WithLongText.args = {
  text: "This is a card with a long text, very very long text message. Just an example how ",
};

export const WithCloseButton = Template.bind({});
WithCloseButton.args = {
  text: "This is a card with a close button",
  onClose: () => {
    console.log("Closed!");
  },
};

export const WithActionButton = Template.bind({});
WithActionButton.args = {
  text: "This is a card with an action button button",
  onAction: () => console.log("Action btn clicked!"),
  actionBtnText: "Click me!",
};

export const WithActionAndCloseButton = Template.bind({});
WithActionAndCloseButton.args = {
  text: "This is a card with an action button button",
  onAction: () => console.log("Action btn clicked!"),
  actionBtnText: "Click me!",
  onClose: () => console.log("Closed!"),
};

export const WarningToast = Template.bind({});
WarningToast.args = {
  text: "This is a card with a close button",
  onClose: () => console.log("Closed!"),
  type: MessageType.WARNING,
};

export const ErrorToast = Template.bind({});
ErrorToast.args = {
  text: "This is a card with a close button",
  onClose: () => console.log("Closed!"),
  type: MessageType.ERROR,
};

export const SuccessToast = Template.bind({});
SuccessToast.args = {
  text: "This is a card with a close button",
  onClose: () => console.log("Closed!"),
  type: MessageType.SUCCESS,
};
