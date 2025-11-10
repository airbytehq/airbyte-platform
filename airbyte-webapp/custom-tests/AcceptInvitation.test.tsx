import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { AcceptInvitation } from "../src/packages/cloud/AcceptInvitation";

// Mocks de dependencias
jest.mock("react-router-dom", () => ({
  useSearchParams: jest.fn(),
  Navigate: ({ to }: any) => <div data-testid="navigate">Navigate to: {to}</div>,
}));

jest.mock("core/api", () => ({
  useAcceptUserInvitation: jest.fn(),
}));

jest.mock("pages/routePaths", () => ({
  RoutePaths: { Workspaces: "workspaces" },
}));

const { useSearchParams } = jest.requireMock("react-router-dom");
const { useAcceptUserInvitation } = jest.requireMock("core/api");

describe("AcceptInvitation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("llama a useAcceptUserInvitation con el inviteCode del query param", () => {
    const mockSearchParams = { get: jest.fn().mockReturnValue("abc123") };
    (useSearchParams as jest.Mock).mockReturnValue([mockSearchParams]);
    (useAcceptUserInvitation as jest.Mock).mockReturnValue({ scopeType: "workspace", scopeId: "w1" });

    render(<AcceptInvitation />);

    expect(mockSearchParams.get).toHaveBeenCalledWith("inviteCode");
    expect(useAcceptUserInvitation).toHaveBeenCalledWith("abc123");
  });

  it("navega al workspace si la invitación tiene scopeType 'workspace'", () => {
    const mockSearchParams = { get: jest.fn().mockReturnValue("abc123") };
    (useSearchParams as jest.Mock).mockReturnValue([mockSearchParams]);
    (useAcceptUserInvitation as jest.Mock).mockReturnValue({ scopeType: "workspace", scopeId: "w123" });

    render(<AcceptInvitation />);

    expect(screen.getByTestId("navigate")).toHaveTextContent("/workspaces/w123");
  });

  it("navega a /workspaces/ sin ID si la invitación no es de tipo 'workspace'", () => {
    const mockSearchParams = { get: jest.fn().mockReturnValue("xyz999") };
    (useSearchParams as jest.Mock).mockReturnValue([mockSearchParams]);
    (useAcceptUserInvitation as jest.Mock).mockReturnValue({ scopeType: "organization", scopeId: "org55" });

    render(<AcceptInvitation />);

    expect(screen.getByTestId("navigate")).toHaveTextContent("/workspaces/");
  });
});
