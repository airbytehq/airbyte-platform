import { StoryObj } from "@storybook/react";

import { Toast } from "./Toast";

export default {
  title: "UI/Toast",
  component: Toast,
  argTypes: {
    text: { type: { name: "string", required: false } },
    type: { type: { name: "string", required: false } },
    onAction: { table: { disable: true } },
    actionBtnText: { type: { name: "string", required: false } },
    onClose: { table: { disable: true } },
  },
} as StoryObj<typeof Toast>;

export const Info: StoryObj<typeof Toast> = {
  args: {
    text: "This is a toast with the variant 'info' ",
  },
};

export const WithCloseButton: StoryObj<typeof Toast> = {
  args: {
    text: "This is a card with a close button",
    onAction: undefined,
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
  name: "Info (close button)",
};

export const WithActionButton: StoryObj<typeof Toast> = {
  args: {
    actionBtnText: "Click me!",
    text: "This is a card with an action button button",
    onClose: undefined,
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
  name: "Info (action button)",
};

export const WithActionAndCloseButton: StoryObj<typeof Toast> = {
  args: {
    text: "This is a card with an action button button",
    actionBtnText: "Click me!",
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
  name: "Info (action + close button)",
};

export const Warning: StoryObj<typeof Toast> = {
  args: {
    text: "This is a card with a close button",
    type: "warning",
    onAction: undefined,
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
};

export const ErrorVariant: StoryObj<typeof Toast> = {
  args: {
    text: "This is a card with a close button",
    type: "error",
    onAction: undefined,
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
  name: "Error",
};

export const ErrorVariantLongMessage: StoryObj<typeof Toast> = {
  args: {
    text: "This is a card with a very long message. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Fusce libero augue, ultrices eu libero vel, blandit feugiat lorem. Curabitur lobortis diam est, non sollicitudin neque venenatis vel. Suspendisse tempus tortor vitae maximus dapibus. Vivamus non tristique lacus, et eleifend orci. Phasellus dictum, dolor in egestas porttitor, elit turpis maximus mi, consequat malesuada erat tellus nec diam. Cras mi diam, lacinia sed congue in, feugiat et nisi. Donec velit nunc, mollis et accumsan nec, malesuada vel odio. Phasellus ac massa quis mauris consectetur fermentum. Praesent semper est sed fringilla euismod. Maecenas sed elementum lacus. Ut id mi magna.",
    type: "error",
    onAction: undefined,
  },
  parameters: {
    actions: { argTypesRegex: "^on.*" },
  },
  name: "Error (long message)",
};

export const SuccessToast: StoryObj<typeof Toast> = {
  args: {
    text: "This is a success toast",
    type: "success",
  },
};

export const Timeout: StoryObj<typeof Toast> = {
  args: {
    text: "This is toast with a timeout",
    type: "success",
    timeout: true,
  },
};
