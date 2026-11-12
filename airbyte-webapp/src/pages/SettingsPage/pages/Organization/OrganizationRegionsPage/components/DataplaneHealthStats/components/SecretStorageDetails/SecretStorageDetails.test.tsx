import { screen } from "@testing-library/react";
import React from "react";

import { render } from "test-utils";

import { SecretStorageDetails } from "./SecretStorageDetails";
import { SecretStorageRow } from "./types";

const mockOnScopeMount = jest.fn();

jest.mock("./ConfiguredFor", () => {
  const { useEffect } = jest.requireActual<typeof React>("react");

  const MockConfiguredFor: React.FC<{ scope: { type: string; id: string } }> = ({ scope }) => {
    useEffect(() => {
      mockOnScopeMount();
    }, []);

    return <>{`${scope.type} · Acme`}</>;
  };

  return { ConfiguredFor: MockConfiguredFor };
});

const SCOPE_ID = "11111111-1111-4111-8111-111111111111";

const row = (overrides: Partial<SecretStorageRow>): SecretStorageRow => ({
  id: "store-1",
  level: "success",
  name: "Store X",
  scope: { type: "organization", id: SCOPE_ID },
  lastChecked: {
    messageId: "settings.organization.regions.dataplane.checkIn.minutes",
    values: { minutes: 12, seconds: 5 },
  },
  ...overrides,
});

describe("SecretStorageDetails", () => {
  describe("with one store", () => {
    beforeEach(async () => {
      await render(<SecretStorageDetails checkIntervalMinutes={30} rows={[row({})]} />);
    });

    it("shows the store name", () => {
      expect(screen.getByText("Store X")).toBeInTheDocument();
    });

    it("shows what the store is configured for", () => {
      expect(screen.getByText("organization · Acme")).toBeInTheDocument();
    });

    it("shows the scope id", () => {
      expect(screen.getByText(SCOPE_ID)).toBeInTheDocument();
    });

    it("shows when the store was last checked", () => {
      expect(screen.getByText("12m 5s ago")).toBeInTheDocument();
    });
  });

  describe("with a store that has no name", () => {
    it("shows the store id", async () => {
      await render(<SecretStorageDetails checkIntervalMinutes={30} rows={[row({ id: "store-9", name: undefined })]} />);

      expect(screen.getByText("store-9")).toBeInTheDocument();
    });
  });

  describe("with a store that has no known scope", () => {
    it("says the scope is unknown", async () => {
      await render(<SecretStorageDetails checkIntervalMinutes={30} rows={[row({ scope: undefined })]} />);

      expect(screen.getByText("Unknown")).toBeInTheDocument();
    });
  });

  describe("with stores at every level", () => {
    beforeEach(async () => {
      await render(
        <SecretStorageDetails
          checkIntervalMinutes={30}
          rows={[
            row({ id: "1", name: "Passed store", level: "success" }),
            row({
              id: "2",
              name: "Failed store",
              level: "error",
              error: { code: "ACCESS_DENIED", message: "Resource policy denied the secret read." },
            }),
            row({
              id: "3",
              name: "Incomplete store",
              level: "warning",
              error: { code: "TIMEOUT", message: "The secret read did not complete within 10 seconds." },
            }),
          ]}
        />
      );
    });

    it("shows the error message of a failed store", () => {
      expect(screen.getByText(/Resource policy denied the secret read\./)).toBeInTheDocument();
    });

    it("shows the error message of a store that could not complete", () => {
      expect(screen.getByText(/The secret read did not complete within 10 seconds\./)).toBeInTheDocument();
    });

    it("shows an error line only for the stores that have an error", () => {
      expect(screen.getAllByRole("row")).toHaveLength(6);
    });
  });

  describe("when the rows are rebuilt for a re-render", () => {
    beforeEach(async () => {
      mockOnScopeMount.mockClear();
      const { rerender } = await render(<SecretStorageDetails checkIntervalMinutes={30} rows={[row({})]} />);
      // The render helper wraps the UI in a div, so the re-render has to as well, or the whole tree remounts.
      rerender(
        <div>
          <SecretStorageDetails checkIntervalMinutes={30} rows={[row({})]} />
        </div>
      );
    });

    it("keeps the cells mounted", () => {
      expect(mockOnScopeMount).toHaveBeenCalledTimes(1);
    });
  });
});
