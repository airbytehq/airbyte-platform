import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

import { DataplaneHealthStats } from "./DataplaneHealthStats";

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  enabled: true,
  ...overrides,
});

const renderStats = (overrides: Partial<DataplaneHealthRead>) =>
  render(<DataplaneHealthStats dataplane={dataplane(overrides)} />);

describe("DataplaneHealthStats", () => {
  describe("for a connected data plane", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now() - 31_000, dataplane_version: "2.4.0" });
    });

    it("shows the name of the data plane", () => {
      expect(screen.getByText("us-east-1-flex-lab")).toBeInTheDocument();
    });

    it("shows the health status", () => {
      expect(screen.getByText("Healthy")).toBeInTheDocument();
    });

    it("shows that it is enabled", () => {
      expect(screen.getByText("Enabled")).toBeInTheDocument();
    });

    it("shows the relative check-in", () => {
      expect(screen.getByText("31 seconds ago")).toBeInTheDocument();
    });

    it("shows the version", () => {
      expect(screen.getByText("2.4.0")).toBeInTheDocument();
    });
  });

  describe("with a one second old check-in", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now() - 1_000 });
    });

    it("uses the singular", () => {
      expect(screen.getByText("1 second ago")).toBeInTheDocument();
    });
  });

  describe("for a data plane without check-ins or version", () => {
    beforeEach(async () => {
      await renderStats({ status: "UNKNOWN", enabled: false });
    });

    it("shows never connected in the badge, both tiles and the secret store card", () => {
      expect(screen.getAllByText("Never connected")).toHaveLength(4);
    });

    it("shows that it is disabled", () => {
      expect(screen.getByText("Disabled")).toBeInTheDocument();
    });

    it("shows an empty health history", () => {
      expect(screen.getByTestId("health-history-empty")).toBeInTheDocument();
    });
  });

  describe("when the server omits the enabled field", () => {
    beforeEach(async () => {
      await renderStats({ enabled: undefined });
    });

    it("shows no enabled badge", () => {
      expect(screen.queryByText("Enabled")).not.toBeInTheDocument();
    });

    it("shows no disabled badge", () => {
      expect(screen.queryByText("Disabled")).not.toBeInTheDocument();
    });
  });

  describe("for a connected data plane that reported no version", () => {
    beforeEach(async () => {
      await renderStats({ status: "UNHEALTHY", last_heartbeat_timestamp: 1, dataplane_version: "unknown" });
    });

    it("shows the health status", () => {
      expect(screen.getByText("Unhealthy")).toBeInTheDocument();
    });

    it("shows the version as unavailable", () => {
      expect(screen.getByText("Unavailable")).toBeInTheDocument();
    });

    it("does not show it as never connected", () => {
      expect(screen.queryByText("Never connected")).not.toBeInTheDocument();
    });
  });

  describe("when the server omits the version for a connected data plane", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now() });
    });

    it("shows the version as unavailable", () => {
      expect(screen.getByText("Unavailable")).toBeInTheDocument();
    });
  });
});
