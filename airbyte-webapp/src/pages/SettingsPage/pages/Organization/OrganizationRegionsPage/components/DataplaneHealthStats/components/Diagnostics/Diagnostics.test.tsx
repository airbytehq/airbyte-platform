import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { mocked, render } from "test-utils";

import { useGetWorkspace, useOrganization } from "core/api";
import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { Diagnostics } from "./Diagnostics";

jest.mock("core/api", () => ({
  useGetWorkspace: jest.fn(),
  useOrganization: jest.fn(),
}));

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  last_heartbeat_timestamp: Date.now(),
  ...overrides,
});

const secretStorage = (
  status: DataplaneDiagnosticStatus,
  overrides: Partial<DataplaneDiagnosticRead> = {}
): DataplaneDiagnosticRead => ({
  kind: DataplaneDiagnosticKind.SECRET_STORAGE,
  target_id: "11111111-1111-4111-8111-111111111111",
  status,
  checked_at: Date.now() - 60_000,
  updated_at: Date.now(),
  observation: { scope_type: "workspace", scope_id: "99999999-9999-4999-8999-999999999999" },
  ...overrides,
});

const VIEW_DETAILS = { name: "View details" };
const HIDE_DETAILS = { name: "Hide details" };

describe("Diagnostics", () => {
  beforeEach(() => {
    mocked(useGetWorkspace).mockReset();
    mocked(useOrganization).mockReset();
  });

  describe("for a connected data plane with a secret store result", () => {
    beforeEach(async () => {
      await render(
        <Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] })} />
      );
    });

    it("shows the Diagnostics heading", () => {
      expect(screen.getByText("Diagnostics")).toBeInTheDocument();
    });

    it("offers to view the details", () => {
      expect(screen.getByRole("button", VIEW_DETAILS)).toBeInTheDocument();
    });

    it("keeps the details closed at first", () => {
      expect(screen.queryByRole("table")).not.toBeInTheDocument();
    });
  });

  describe("the summary of the secret stores", () => {
    it.each([
      [DataplaneDiagnosticStatus.PASSED, "Yes"],
      [DataplaneDiagnosticStatus.FAILED, "No"],
      [DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, "Incomplete"],
    ])("shows a store that %s as %s", async (status, value) => {
      await render(<Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(status)] })} />);

      expect(screen.getByText(value)).toBeInTheDocument();
    });

    it("says how long ago the stores were observed", async () => {
      await render(
        <Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] })} />
      );

      expect(screen.getByText(/^Observed 1m \d+s ago$/)).toBeInTheDocument();
    });
  });

  describe("after the details are opened", () => {
    beforeEach(async () => {
      await render(
        <Diagnostics
          dataplane={dataplane({
            diagnostics: [
              secretStorage(DataplaneDiagnosticStatus.FAILED, {
                error_code: "ACCESS_DENIED",
                error_message: "Resource policy denied the secret read.",
              }),
            ],
          })}
        />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
    });

    it("says how often each store is checked", () => {
      expect(screen.getByText("Secrets manager details · each store is checked every 30 minutes")).toBeInTheDocument();
    });

    it("shows the details table", () => {
      expect(screen.getByRole("table")).toBeInTheDocument();
    });

    it("offers to hide the details", () => {
      expect(screen.getByRole("button", HIDE_DETAILS)).toBeInTheDocument();
    });

    it("links the button to the panel that holds the details", () => {
      const panelId = screen.getByRole("button", HIDE_DETAILS).getAttribute("aria-controls") ?? "";
      expect(document.getElementById(panelId)).toContainElement(screen.getByRole("table"));
    });

    it("lists the store by its id", () => {
      expect(screen.getByText("11111111-1111-4111-8111-111111111111")).toBeInTheDocument();
    });

    it("shows what the store is configured for", () => {
      expect(screen.getByRole("cell", { name: /Workspace/ })).toBeInTheDocument();
    });

    it("shows the scope id", () => {
      expect(screen.getByText("99999999-9999-4999-8999-999999999999")).toBeInTheDocument();
    });

    it("shows the error code of the store", () => {
      expect(screen.getByText("ACCESS_DENIED")).toBeInTheDocument();
    });

    it("shows the error message of the store", () => {
      expect(screen.getByText(/Resource policy denied the secret read\./)).toBeInTheDocument();
    });
  });

  describe("after the details are opened and closed again", () => {
    beforeEach(async () => {
      await render(
        <Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] })} />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
      await userEvent.click(screen.getByRole("button", HIDE_DETAILS));
    });

    it("hides the details table", () => {
      expect(screen.queryByRole("table")).not.toBeInTheDocument();
    });

    it("offers to view the details again", () => {
      expect(screen.getByRole("button", VIEW_DETAILS)).toBeInTheDocument();
    });
  });

  describe("with a store whose scope is not understood", () => {
    beforeEach(async () => {
      await render(
        <Diagnostics
          dataplane={dataplane({
            diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED, { observation: { scope_type: "OTHER" } })],
          })}
        />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
    });

    it("shows the scope as unknown", () => {
      expect(within(screen.getByRole("table")).getByText("Unknown")).toBeInTheDocument();
    });
  });

  describe("when the workspace the store is configured for has a name", () => {
    beforeEach(async () => {
      mocked(useGetWorkspace).mockReturnValue({ name: "Finance" } as never);
      await render(
        <Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] })} />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
    });

    it("shows the type and the name of the workspace", () => {
      expect(screen.getByText("Workspace · Finance")).toBeInTheDocument();
    });
  });

  describe("when the name of the scope cannot be fetched", () => {
    let consoleError: jest.SpyInstance;

    beforeEach(async () => {
      consoleError = jest.spyOn(console, "error").mockImplementation(() => {});
      mocked(useGetWorkspace).mockImplementation(() => {
        throw new Error("Forbidden");
      });
      await render(
        <Diagnostics dataplane={dataplane({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] })} />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
    });

    afterEach(() => {
      consoleError.mockRestore();
    });

    it("still lists the store", () => {
      expect(screen.getByText("11111111-1111-4111-8111-111111111111")).toBeInTheDocument();
    });
  });

  describe("when the health data names the store", () => {
    beforeEach(async () => {
      await render(
        <Diagnostics
          dataplane={dataplane({
            diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED, { target_name: "Acme vault" })],
          })}
        />
      );
      await userEvent.click(screen.getByRole("button", VIEW_DETAILS));
    });

    it("lists the store by its name", () => {
      expect(screen.getByText("Acme vault")).toBeInTheDocument();
    });

    it("does not list the store by its id", () => {
      expect(screen.queryByText("11111111-1111-4111-8111-111111111111")).not.toBeInTheDocument();
    });
  });

  describe("for a data plane that never connected", () => {
    beforeEach(async () => {
      await render(<Diagnostics dataplane={dataplane({ last_heartbeat_timestamp: undefined, status: "UNKNOWN" })} />);
    });

    it("shows the card", () => {
      expect(screen.getByText("Secrets manager reachable")).toBeInTheDocument();
    });

    it("offers no details", () => {
      expect(screen.queryByRole("button")).not.toBeInTheDocument();
    });
  });

  describe("for a connected data plane with no secret store result", () => {
    beforeEach(async () => {
      await render(<Diagnostics dataplane={dataplane({ diagnostics: [] })} />);
    });

    it("shows nothing", () => {
      expect(screen.queryByText("Diagnostics")).not.toBeInTheDocument();
    });
  });
});
