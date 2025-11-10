import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import "@testing-library/jest-dom";
import { JobFailureDetails } from "../src/area/connection/components/JobHistoryItem/JobFailureDetails";

declare const require: any;

// Mocks de dependencias
jest.mock("react-intl", () => ({
  FormattedMessage: ({ id }: any) => <span>{id}</span>,
  useIntl: () => ({
    formatMessage: ({ id }: any, values?: any) =>
      `${id} ${values?.type?.props?.children || ""} ${values?.message || ""}`,
  }),
}));

jest.mock("components/ui/Box", () => ({
  Box: (props: any) => <div data-testid="box">{props.children}</div>,
}));

jest.mock("components/ui/Flex", () => ({
  FlexContainer: (props: any) => <div data-testid="flex">{props.children}</div>,
}));


jest.mock("components/ui/Text", () => {
  const React = require("react");
  return {
    Text: React.forwardRef(({ children, ...props }: any, ref: any) => (
      <div ref={ref} data-testid="text" {...props}>
        {children}
      </div>
    )),
  };
});

jest.mock("components/ui/Button", () => ({
  Button: ({ children, onClick }: any) => (
    <button data-testid="button" onClick={onClick}>
      {children}
    </button>
  ),
}));

jest.mock("components/ui/Icon", () => ({
  Icon: ({ type }: any) => <span data-testid="icon">{type}</span>,
}));


describe("JobFailureDetails", () => {
  const baseFailure = {
    type: "error",
    typeLabel: "Error",
    message: "Main failure message",
    secondaryMessage: "Secondary details message",
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("renderiza el mensaje principal correctamente", () => {
    render(<JobFailureDetails failureUiDetails={baseFailure} />);
    expect(screen.getByTestId("box")).toBeInTheDocument();
    expect(screen.getByText(/failureMessage.label/i)).toBeInTheDocument();
    expect(screen.getByText(/Main failure message/)).toBeInTheDocument();
  });

  it("muestra el botón 'see more' cuando hay mensaje secundario", () => {
    render(<JobFailureDetails failureUiDetails={baseFailure} />);
    expect(screen.getByText("jobs.failure.seeMore")).toBeInTheDocument();
    expect(screen.getByTestId("icon")).toHaveTextContent("chevronRight");
  });

  it("expande los detalles al hacer clic en 'see more'", () => {
    render(<JobFailureDetails failureUiDetails={baseFailure} />);
    const button = screen.getByRole("button");
    fireEvent.click(button);

    expect(screen.getByText("jobs.failure.seeLess")).toBeInTheDocument();
    expect(screen.getByTestId("icon")).toHaveTextContent("chevronDown");
    expect(screen.getByText("Secondary details message")).toBeInTheDocument();
  });

  it("colapsa el mensaje al volver a hacer clic en 'see less'", () => {
    render(<JobFailureDetails failureUiDetails={baseFailure} />);
    const button = screen.getByRole("button");

    fireEvent.click(button);
    expect(screen.getByText("jobs.failure.seeLess")).toBeInTheDocument();

    fireEvent.click(button);
    expect(screen.getByText("jobs.failure.seeMore")).toBeInTheDocument();
  });
});
