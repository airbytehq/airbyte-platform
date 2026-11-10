import { screen, within } from "@testing-library/react";

import { render } from "test-utils";

import {
  DataplaneDiagnosticKind,
  DataplaneDiagnosticRead,
  DataplaneDiagnosticStatus,
  DataplaneHealthRead,
} from "core/api/types/AirbyteClient";

import { DataplaneHealthStats } from "./DataplaneHealthStats";
import { formatAge } from "./utils";

const dataplane = (overrides: Partial<DataplaneHealthRead>): DataplaneHealthRead => ({
  dataplane_id: "dp-1",
  dataplane_name: "us-east-1-flex-lab",
  dataplane_group_id: "group-1",
  organization_id: "org-1",
  status: "HEALTHY",
  enabled: true,
  ...overrides,
});

describe("formatAge", () => {
  it("picks the unit from the age", () => {
    expect(formatAge(31_000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.seconds",
      values: { seconds: 31 },
    });
    expect(formatAge(72_000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.minutes",
      values: { minutes: 1, seconds: 12 },
    });
    expect(formatAge((60 * 60 + 13 * 60 + 12) * 1000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.hours",
      values: { hours: 1, minutes: 13, seconds: 12 },
    });
    expect(formatAge((26 * 60 * 60 + 5) * 1000)).toEqual({
      messageId: "settings.organization.regions.dataplane.checkIn.days",
      values: { days: 1, hours: 2 },
    });
  });

  it("never reports a negative age", () => {
    expect(formatAge(-5000).values).toEqual({ seconds: 0 });
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

    expect(screen.getAllByText("Never connected")).toHaveLength(4);
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

const cardFor = (label: string): HTMLElement => {
  const card = screen.getByText(label).parentElement;
  if (!card) {
    throw new Error(`No card found for "${label}"`);
  }
  return card;
};

const iconOf = (label: string) => {
  const icon = cardFor(label).querySelector("[data-icon]");
  const color = icon && [...icon.classList].find((className) => className.startsWith("icon--"));
  return { name: icon?.getAttribute("data-icon"), color: color?.replace("icon--", "") };
};

const renderStats = (overrides: Partial<DataplaneHealthRead>) =>
  render(<DataplaneHealthStats dataplane={dataplane(overrides)} />);

describe("DataplaneHealthStats tiles", () => {
  describe("for a healthy data plane", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now() });
    });

    it("shows the success icon on the last check-in", () => {
      expect(iconOf("Last check-in")).toEqual({ name: "status-success", color: "success" });
    });
  });

  describe("for a degraded data plane", () => {
    beforeEach(async () => {
      await renderStats({ status: "DEGRADED", last_heartbeat_timestamp: Date.now() });
    });

    it("shows the warning icon on the last check-in", () => {
      expect(iconOf("Last check-in")).toEqual({ name: "status-warning", color: "warning" });
    });
  });

  describe("for a data plane that never connected", () => {
    beforeEach(async () => {
      await renderStats({ status: "UNKNOWN" });
    });

    it("shows the inactive icon on the last check-in", () => {
      expect(iconOf("Last check-in")).toEqual({ name: "status-error", color: "disabled" });
    });

    it("shows the inactive icon on the Airbyte version", () => {
      expect(iconOf("Airbyte version")).toEqual({ name: "status-error", color: "disabled" });
    });
  });

  describe("with a known version", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now(), dataplane_version: "2.4.0" });
    });

    it("shows the success icon on the version", () => {
      expect(iconOf("Airbyte version")).toEqual({ name: "status-success", color: "success" });
    });
  });

  describe("with an unavailable version", () => {
    beforeEach(async () => {
      await renderStats({ last_heartbeat_timestamp: Date.now() });
    });

    it("shows the neutral icon on the version", () => {
      expect(iconOf("Airbyte version")).toEqual({ name: "status-error", color: "disabled" });
    });
  });
});

describe("DataplaneHealthStats secret store diagnostic", () => {
  const SECRETS_LABEL = "Secrets manager reachable";

  const secretStorage = (
    status: DataplaneDiagnosticStatus,
    overrides: Partial<DataplaneDiagnosticRead> = {}
  ): DataplaneDiagnosticRead => ({
    kind: DataplaneDiagnosticKind.SECRET_STORAGE,
    target_id: "store-1",
    status,
    checked_at: Date.now() - 60_000,
    updated_at: Date.now(),
    ...overrides,
  });

  const renderConnected = (overrides: Partial<DataplaneHealthRead>) =>
    renderStats({ last_heartbeat_timestamp: Date.now(), ...overrides });

  describe("without diagnostics", () => {
    beforeEach(async () => {
      await renderConnected({});
    });

    it("hides the Diagnostics heading", () => {
      expect(screen.queryByText("Diagnostics")).not.toBeInTheDocument();
    });

    it("hides the secret store card", () => {
      expect(screen.queryByText(SECRETS_LABEL)).not.toBeInTheDocument();
    });
  });

  describe("with an empty diagnostics list", () => {
    beforeEach(async () => {
      await renderConnected({ diagnostics: [] });
    });

    it("hides the Diagnostics heading", () => {
      expect(screen.queryByText("Diagnostics")).not.toBeInTheDocument();
    });

    it("hides the secret store card", () => {
      expect(screen.queryByText(SECRETS_LABEL)).not.toBeInTheDocument();
    });
  });

  describe("when every store passed", () => {
    beforeEach(async () => {
      await renderConnected({ diagnostics: [secretStorage(DataplaneDiagnosticStatus.PASSED)] });
    });

    it("shows the Diagnostics heading", () => {
      expect(screen.getByText("Diagnostics")).toBeInTheDocument();
    });

    it("shows Yes", () => {
      expect(within(cardFor(SECRETS_LABEL)).getByText("Yes")).toBeInTheDocument();
    });

    it("shows the success icon", () => {
      expect(iconOf(SECRETS_LABEL)).toEqual({ name: "status-success", color: "success" });
    });
  });

  describe("when a store failed", () => {
    beforeEach(async () => {
      await renderConnected({
        diagnostics: [
          secretStorage(DataplaneDiagnosticStatus.PASSED),
          secretStorage(DataplaneDiagnosticStatus.FAILED, { target_id: "store-2" }),
        ],
      });
    });

    it("shows No", () => {
      expect(within(cardFor(SECRETS_LABEL)).getByText("No")).toBeInTheDocument();
    });

    it("shows the error icon", () => {
      expect(iconOf(SECRETS_LABEL)).toEqual({ name: "status-error", color: "error" });
    });
  });

  describe("when no store failed but one could not complete", () => {
    beforeEach(async () => {
      await renderConnected({
        diagnostics: [
          secretStorage(DataplaneDiagnosticStatus.PASSED),
          secretStorage(DataplaneDiagnosticStatus.UNABLE_TO_COMPLETE, { target_id: "store-2" }),
        ],
      });
    });

    it("shows Incomplete", () => {
      expect(within(cardFor(SECRETS_LABEL)).getByText("Incomplete")).toBeInTheDocument();
    });

    it("shows the warning icon", () => {
      expect(iconOf(SECRETS_LABEL)).toEqual({ name: "status-warning", color: "warning" });
    });
  });

  describe("when the stores were checked at different times", () => {
    beforeEach(async () => {
      await renderConnected({
        diagnostics: [
          secretStorage(DataplaneDiagnosticStatus.PASSED, { checked_at: Date.now() - 26 * 60 * 60 * 1000 }),
          secretStorage(DataplaneDiagnosticStatus.PASSED, { target_id: "store-2", checked_at: Date.now() - 60_000 }),
        ],
      });
    });

    it("shows the age of the oldest result", () => {
      expect(within(cardFor(SECRETS_LABEL)).getByText("Observed 1d 2h ago")).toBeInTheDocument();
    });
  });

  describe("for a data plane that never connected", () => {
    beforeEach(async () => {
      await renderStats({ status: "UNKNOWN" });
    });

    it("shows the Diagnostics heading", () => {
      expect(screen.getByText("Diagnostics")).toBeInTheDocument();
    });

    it("shows Never connected on the secret store card", () => {
      expect(within(cardFor(SECRETS_LABEL)).getByText("Never connected")).toBeInTheDocument();
    });

    it("shows the neutral icon", () => {
      expect(iconOf(SECRETS_LABEL)).toEqual({ name: "status-error", color: "disabled" });
    });

    it("shows no observed time", () => {
      expect(screen.queryByText(/^Observed /)).not.toBeInTheDocument();
    });
  });
});
