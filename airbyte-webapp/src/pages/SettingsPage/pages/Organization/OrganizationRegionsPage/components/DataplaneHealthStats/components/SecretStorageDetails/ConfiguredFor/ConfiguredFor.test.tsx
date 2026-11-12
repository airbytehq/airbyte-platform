import { screen } from "@testing-library/react";

import { mocked, render } from "test-utils";

import { useGetWorkspace, useOrganization } from "core/api";

import { ConfiguredFor } from "./ConfiguredFor";

jest.mock("core/api", () => ({
  useGetWorkspace: jest.fn(),
  useOrganization: jest.fn(),
}));

const WORKSPACE = { type: "workspace", id: "workspace-1" } as const;
const ORGANIZATION = { type: "organization", id: "org-1" } as const;

describe("ConfiguredFor", () => {
  let consoleError: jest.SpyInstance;

  beforeEach(() => {
    mocked(useGetWorkspace).mockReset();
    mocked(useOrganization).mockReset();
    // React logs an error that an error boundary catches, which would only add noise here.
    consoleError = jest.spyOn(console, "error").mockImplementation(() => {});
  });

  afterEach(() => {
    consoleError.mockRestore();
  });

  describe("for a workspace with a name", () => {
    beforeEach(async () => {
      mocked(useGetWorkspace).mockReturnValue({ name: "Finance" } as never);
      await render(<ConfiguredFor scope={WORKSPACE} />);
    });

    it("shows the type and the name", () => {
      expect(screen.getByText("Workspace · Finance")).toBeInTheDocument();
    });

    it("asks for the workspace", () => {
      expect(useGetWorkspace).toHaveBeenCalledWith("workspace-1");
    });

    it("does not ask for an organization", () => {
      expect(useOrganization).not.toHaveBeenCalled();
    });
  });

  describe("for an organization with a name", () => {
    beforeEach(async () => {
      mocked(useOrganization).mockReturnValue({ organizationName: "Acme" } as never);
      await render(<ConfiguredFor scope={ORGANIZATION} />);
    });

    it("shows the type and the name", () => {
      expect(screen.getByText("Organization · Acme")).toBeInTheDocument();
    });

    it("asks for the organization", () => {
      expect(useOrganization).toHaveBeenCalledWith("org-1");
    });

    it("does not ask for a workspace", () => {
      expect(useGetWorkspace).not.toHaveBeenCalled();
    });
  });

  describe("for a workspace that was not found", () => {
    beforeEach(async () => {
      mocked(useGetWorkspace).mockReturnValue(undefined);
      await render(<ConfiguredFor scope={WORKSPACE} />);
    });

    it("shows the id in place of the name", () => {
      expect(screen.getByText("Workspace · workspace-1")).toBeInTheDocument();
    });
  });

  describe("while the workspace is loading", () => {
    beforeEach(async () => {
      mocked(useGetWorkspace).mockImplementation(() => {
        throw new Promise(() => {});
      });
      await render(<ConfiguredFor scope={WORKSPACE} />);
    });

    it("shows the type", () => {
      expect(screen.getByText("Workspace ·")).toBeInTheDocument();
    });

    it("does not show the id yet", () => {
      expect(screen.queryByText(/workspace-1/)).not.toBeInTheDocument();
    });
  });

  describe("when the workspace cannot be fetched", () => {
    beforeEach(async () => {
      mocked(useGetWorkspace).mockImplementation(() => {
        throw new Error("Forbidden");
      });
      await render(<ConfiguredFor scope={WORKSPACE} />);
    });

    it("shows the id in place of the name", () => {
      expect(screen.getByText("Workspace · workspace-1")).toBeInTheDocument();
    });
  });

  describe("when the organization cannot be fetched", () => {
    beforeEach(async () => {
      mocked(useOrganization).mockImplementation(() => {
        throw new Error("Forbidden");
      });
      await render(<ConfiguredFor scope={ORGANIZATION} />);
    });

    it("shows the id in place of the name", () => {
      expect(screen.getByText("Organization · org-1")).toBeInTheDocument();
    });
  });
});
