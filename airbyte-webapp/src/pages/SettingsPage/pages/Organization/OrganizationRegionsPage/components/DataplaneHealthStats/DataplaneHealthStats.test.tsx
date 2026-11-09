import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { DataplaneHealthRead } from "core/api/types/AirbyteClient";

import { DataplaneHealthStats, formatCheckInAge } from "./DataplaneHealthStats";

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  enabled: true,
  ...overrides,
});

describe("formatCheckInAge", () => {
  it("picks the unit from the age", () => {
    expect(formatCheckInAge(31_000)).toEqual({
      id: "settings.organization.regions.dataplane.checkIn.seconds",
      values: { seconds: 31 },
    });
    expect(formatCheckInAge(72_000)).toEqual({
      id: "settings.organization.regions.dataplane.checkIn.minutes",
      values: { minutes: 1, seconds: 12 },
    });
    expect(formatCheckInAge((60 * 60 + 13 * 60 + 12) * 1000)).toEqual({
      id: "settings.organization.regions.dataplane.checkIn.hours",
      values: { hours: 1, minutes: 13, seconds: 12 },
    });
    expect(formatCheckInAge((26 * 60 * 60 + 5) * 1000)).toEqual({
      id: "settings.organization.regions.dataplane.checkIn.days",
      values: { days: 1, hours: 2 },
    });
  });

  it("never reports a negative age", () => {
    expect(formatCheckInAge(-5000).values).toEqual({ seconds: 0 });
  });
});

describe("DataplaneHealthStats", () => {
  it("shows the status, enabled state, relative check-in and version", async () => {
    await render(
      <DataplaneHealthStats
        dataplane={dataplane({ last_heartbeat_timestamp: Date.now() - 31_000, dataplane_version: "2.4.0" })}
      />
    );

    expect(screen.getByText("us-east-1-flex-lab")).toBeInTheDocument();
    expect(screen.getByText("Healthy")).toBeInTheDocument();
    expect(screen.getByText("Enabled")).toBeInTheDocument();
    expect(screen.getByText("31 seconds ago")).toBeInTheDocument();
    expect(screen.getByText("2.4.0")).toBeInTheDocument();
  });

  it("uses the singular for a one second old check-in", async () => {
    await render(<DataplaneHealthStats dataplane={dataplane({ last_heartbeat_timestamp: Date.now() - 1_000 })} />);

    expect(screen.getByText("1 second ago")).toBeInTheDocument();
  });

  it("shows never connected for a data plane without check-ins or version", async () => {
    await render(<DataplaneHealthStats dataplane={dataplane({ status: "UNKNOWN", enabled: false })} />);

    expect(screen.getAllByText("Never connected")).toHaveLength(3);
    expect(screen.getByText("Disabled")).toBeInTheDocument();
    expect(screen.getByTestId("health-history-empty")).toBeInTheDocument();
  });

  it("shows no enabled badge when the server omits the field", async () => {
    await render(<DataplaneHealthStats dataplane={dataplane({ enabled: undefined })} />);

    expect(screen.queryByText("Enabled")).not.toBeInTheDocument();
    expect(screen.queryByText("Disabled")).not.toBeInTheDocument();
  });

  it("shows the version as unavailable when a connected data plane reported none", async () => {
    await render(
      <DataplaneHealthStats
        dataplane={dataplane({ status: "UNHEALTHY", last_heartbeat_timestamp: 1, dataplane_version: "unknown" })}
      />
    );

    expect(screen.getByText("Unhealthy")).toBeInTheDocument();
    expect(screen.getByText("Unavailable")).toBeInTheDocument();
    expect(screen.queryByText("Never connected")).not.toBeInTheDocument();
  });

  it("shows the version as unavailable when the server omits it for a connected data plane", async () => {
    await render(<DataplaneHealthStats dataplane={dataplane({ last_heartbeat_timestamp: Date.now() })} />);

    expect(screen.getByText("Unavailable")).toBeInTheDocument();
  });
});
