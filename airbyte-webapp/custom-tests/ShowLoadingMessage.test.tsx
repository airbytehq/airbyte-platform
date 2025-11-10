import React from "react";
import { render, screen, act } from "@testing-library/react";
import "@testing-library/jest-dom";
import ShowLoadingMessage from "../src/views/Connector/ConnectorCard/components/ShowLoadingMessage";

// Mock de dependencias
jest.mock("react-intl", () => ({
  FormattedMessage: ({ id, values }: any) => (
    <span data-testid="formatted-message">
      {id}
      {values?.connector && `-${values.connector}`}
    </span>
  ),
}));

jest.mock("components/ui/Link", () => ({
  ExternalLink: ({ href, children }: any) => <a href={href}>{children}</a>,
}));

jest.mock("core/utils/app", () => ({
  useIsCloudApp: () => false,
}));

jest.mock("core/utils/links", () => ({
  links: {
    supportPortal: "https://cloud.support",
    technicalSupport: "https://oss.support",
  },
}));

describe("ShowLoadingMessage", () => {
  beforeEach(() => {
    jest.useFakeTimers();
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it("muestra el mensaje de carga inicialmente", () => {
    render(<ShowLoadingMessage connector="MyConnector" />);
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("form.loadingConfiguration-MyConnector");
  });

  it("muestra el mensaje de 'too long' después de 10 segundos", () => {
    render(<ShowLoadingMessage connector="MyConnector" />);

    act(() => {
      jest.advanceTimersByTime(10000);
    });

    expect(screen.getByTestId("formatted-message")).toHaveTextContent("form.tooLong");
  });

  it("reinicia el temporizador cuando cambia el conector", () => {
    const { rerender } = render(<ShowLoadingMessage connector="A" />);

    act(() => {
      jest.advanceTimersByTime(5000);
    });
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("form.loadingConfiguration-A");

    // Cambiamos de conector
    rerender(<ShowLoadingMessage connector="B" />);

    act(() => {
      jest.advanceTimersByTime(5000);
    });
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("form.loadingConfiguration-B");


    act(() => {
      jest.advanceTimersByTime(5000);
    });
    expect(screen.getByTestId("formatted-message")).toHaveTextContent("form.tooLong");
  });
});
