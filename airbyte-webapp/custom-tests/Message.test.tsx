import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { Message } from "../src/components/chat/Message";

// Mocks de dependencias
jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: { id: string }) => <span data-testid="formatted-message">{id}</span>,
}));

jest.mock("../src/components/chat/SafeMarkdown", () => ({
  SafeMarkdown: ({ content }: { content: string }) => <div data-testid="markdown">{content}</div>,
}));

jest.mock("../src/components/chat/ToolCallItem", () => ({
  ToolCallItem: ({ toolCall }: any) => (
    <div data-testid="toolcall-item">ToolCall: {toolCall.tool_name}</div>
  ),
}));

const baseMessage = {
  id: "1",
  content: "Hola",
  role: "user" as const,
  timestamp: new Date(),
};


describe("Message component", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderiza un mensaje de usuario con contenido y etiqueta traducida", () => {
    render(<Message message={baseMessage} />);

    expect(screen.getByTestId("markdown")).toHaveTextContent("Hola");
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("chat.message.user");
  });


  it("no renderiza nada si el contenido está vacío", () => {
    const { container } = render(<Message message={{ ...baseMessage, content: "" }} />);
    expect(container.firstChild).toBeNull();
  });

  it("no renderiza nada si role='tool' y no hay toolCall", () => {
    const message = {
      ...baseMessage,
      role: "tool" as const,
      toolCall: undefined,
      content: "",
    };
    const { container } = render(<Message message={message} />);
    expect(container.firstChild).toBeNull();
  });

  it("usa ToolCallItem si no hay componente personalizado y showAllToolCalls=true", () => {
    const message = {
      ...baseMessage,
      role: "tool" as const,
      content: "",
      toolCall: { tool_name: "myTool", args: null, call_id: "123" },
    };

    render(<Message message={message} showAllToolCalls />);

    expect(screen.getByTestId("toolcall-item")).toHaveTextContent("myTool");
  });

  it("usa un componente personalizado si está definido en toolComponents", () => {
    const CustomTool = jest.fn(({ toolCall }) => (
      <div data-testid="custom-tool">Custom: {toolCall.tool_name}</div>
    ));
    const message = {
      ...baseMessage,
      role: "tool" as const,
      content: "",
      toolCall: { tool_name: "specialTool", args: {}, call_id: "t1" },
      toolResponse: { tool_name: "specialTool", response: "ok", call_id: "t1" },
    };

    render(<Message message={message} toolComponents={{ specialTool: CustomTool }} />);

    expect(screen.getByTestId("custom-tool")).toHaveTextContent("specialTool");
    expect(CustomTool).toHaveBeenCalled();
  });

  it("no renderiza nada si no hay CustomRenderer y showAllToolCalls=false", () => {
    const message = {
      ...baseMessage,
      role: "tool" as const,
      content: "",
      toolCall: { tool_name: "invisibleTool", args: {}, call_id: "x" },
    };

    const { container } = render(<Message message={message} showAllToolCalls={false} />);
    expect(container.firstChild).toBeNull();
  });

  it("renderiza un mensaje de asistente con markdown y traducción", () => {
    const message = {
      ...baseMessage,
      role: "assistant" as const,
      content: "Mensaje del asistente",
    };

    render(<Message message={message} />);

    expect(screen.getByTestId("markdown")).toHaveTextContent("Mensaje del asistente");
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("chat.message.assistant");
  });
});
