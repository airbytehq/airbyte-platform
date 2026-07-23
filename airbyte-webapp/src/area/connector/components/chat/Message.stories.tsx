import type { Meta, StoryObj } from "@storybook/react";

import { Message, type ChatMessage } from "./Message";

const meta: Meta<typeof Message> = {
  title: "Chat/Message",
  component: Message,
  parameters: {
    layout: "padded",
  },
};

export default meta;
type Story = StoryObj<typeof Message>;

const baseMessage: ChatMessage = {
  id: "1",
  content: "Here is the analysis of your workspace configuration...",
  role: "assistant",
  timestamp: new Date(),
};

// 1. Normal completed message
export const CompletedMessage: Story = {
  args: {
    message: baseMessage,
  },
};

// 2. Streaming with content - shows content with cursor
export const StreamingWithContent: Story = {
  args: {
    message: {
      ...baseMessage,
      isStreaming: true,
    },
  },
};

// 3. Streaming without content - shows ThinkingIndicator
export const StreamingNoContent: Story = {
  args: {
    message: {
      ...baseMessage,
      content: "",
      isStreaming: true,
    },
    showThinkingIndicator: true,
  },
};
