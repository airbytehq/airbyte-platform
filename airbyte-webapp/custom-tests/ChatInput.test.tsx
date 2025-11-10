import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { ChatInput } from "../src/components/chat/ChatInput";

// Mocks de dependencias
jest.mock("react-intl", () => ({
  useIntl: () => ({
    formatMessage: ({ id, defaultMessage }: any) => defaultMessage || id,
  }),
}));

jest.mock("react-use", () => ({
  useToggle: (initial: boolean) => [initial, jest.fn()],
}));

jest.mock("components/ui/Message", () => ({
  Message: ({ text }: any) => <div data-testid="message">{text}</div>,
}));

jest.mock("../src/components/ui/Icon", () => ({
  Icon: ({ type }: { type: string }) => <span data-testid={`icon-${type}`} />,
}));

describe("ChatInput", () => {
  const mockOnSend = jest.fn();
  const mockOnStop = jest.fn();

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("envía un mensaje cuando se presiona Enter y no está deshabilitado", () => {
    render(<ChatInput onSendMessage={mockOnSend} />);

    const textarea = screen.getByRole("textbox");
    fireEvent.change(textarea, { target: { value: "Hola mundo" } });
    fireEvent.keyDown(textarea, { key: "Enter", shiftKey: false });

    expect(mockOnSend).toHaveBeenCalledWith("Hola mundo");
    expect(textarea).toHaveValue("");
  });

  it("no envía el mensaje si está deshabilitado", () => {
    render(<ChatInput onSendMessage={mockOnSend} disabled />);

    const textarea = screen.getByRole("textbox");
    fireEvent.change(textarea, { target: { value: "No debería enviarse" } });
    fireEvent.keyDown(textarea, { key: "Enter", shiftKey: false });

    expect(mockOnSend).not.toHaveBeenCalled();
  });

  it("muestra el botón de stop cuando isStreaming es true", () => {
    render(<ChatInput onSendMessage={mockOnSend} onStop={mockOnStop} isStreaming />);

    const stopButton = screen.getByRole("button", { name: /chat.input.stop/i });
    expect(stopButton).toBeInTheDocument();

    fireEvent.click(stopButton);
    expect(mockOnStop).toHaveBeenCalled();
  });

  it("muestra un textarea normal cuando no está en modo secreto", () => {
    render(<ChatInput onSendMessage={mockOnSend} />);

    const textarea = screen.getByRole("textbox");
    expect(textarea.tagName.toLowerCase()).toBe("textarea");
  });

  it("muestra el mensaje de modo secreto cuando isSecretMode es true", () => {
    render(<ChatInput onSendMessage={mockOnSend} isSecretMode secretFieldName="token" />);
    expect(screen.getByTestId("message")).toBeInTheDocument();
  });

  it("el botón de enviar está deshabilitado si no hay texto", () => {
    render(<ChatInput onSendMessage={mockOnSend} />);
    const sendButton = screen.getByRole("button", { name: /chat.input.send/i });
    expect(sendButton).toBeDisabled();
  });

  it("habilita el botón de enviar al escribir texto", () => {
    render(<ChatInput onSendMessage={mockOnSend} />);
    const textarea = screen.getByRole("textbox");
    const sendButton = screen.getByRole("button", { name: /chat.input.send/i });

    fireEvent.change(textarea, { target: { value: "Mensaje listo" } });
    expect(sendButton).not.toBeDisabled();
  });
});
