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

// Base message for stories
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

// 2. Streaming with content (current behavior)
export const StreamingWithContent: Story = {
  args: {
    message: {
      ...baseMessage,
      isStreaming: true,
    },
  },
};

// 3. Streaming without content - OLD behavior (blank/cursor only)
export const StreamingNoContent_OldBehavior: Story = {
  args: {
    message: {
      ...baseMessage,
      content: "",
      isStreaming: true,
    },
    showStreamingIndicator: false, // Explicitly disabled
  },
};

// 4. Streaming without content - NEW behavior (with indicator)
export const StreamingNoContent_WithIndicator: Story = {
  args: {
    message: {
      ...baseMessage,
      content: "",
      isStreaming: true,
    },
    showStreamingIndicator: true, // NEW feature
  },
};

// 5. Comparison side-by-side (using Storybook layout)
export const Comparison: Story = {
  render: () => (
    <div style={{ display: "flex", gap: "20px" }}>
      <div>
        <h3>Without Indicator (Old)</h3>
        <Message message={{ ...baseMessage, content: "", isStreaming: true }} showStreamingIndicator={false} />
      </div>
      <div>
        <h3>With Indicator (New)</h3>
        <Message message={{ ...baseMessage, content: "", isStreaming: true }} showStreamingIndicator />
      </div>
    </div>
  ),
};
