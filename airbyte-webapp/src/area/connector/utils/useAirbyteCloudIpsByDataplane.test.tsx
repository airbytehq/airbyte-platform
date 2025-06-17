import { render, screen } from "@testing-library/react";
import React from "react";

import { useCurrentWorkspace, useListDataplaneGroups } from "core/api";
import { useExperiment } from "hooks/services/Experiment";

import { useAirbyteCloudIpsByDataplane } from "./useAirbyteCloudIpsByDataplane";

jest.mock("core/api", () => ({
  useCurrentWorkspace: jest.fn(),
  useListDataplaneGroups: jest.fn(),
}));
jest.mock("hooks/services/Experiment", () => ({
  useExperiment: jest.fn(),
}));

class ErrorBoundary extends React.Component<{ children: React.ReactNode }, { error: Error | null }> {
  constructor(props: { children: React.ReactNode }) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return <div data-testid="error">{this.state.error.message}</div>;
    }
    return this.props.children;
  }
}

const TestComponent: React.FC = () => {
  const ips = useAirbyteCloudIpsByDataplane();
  return <div data-testid="ips">{JSON.stringify(ips)}</div>;
};

describe("useAirbyteCloudIpsByDataplane", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("returns default IPs if no dataplane group is found in default experiment value", () => {
    (useCurrentWorkspace as jest.Mock).mockReturnValue({ dataplaneGroupId: "g1" });
    (useListDataplaneGroups as jest.Mock).mockReturnValue([{ dataplane_group_id: "g1", name: "US" }]);
    (useExperiment as jest.Mock).mockReturnValue({ auto: ["1.1.1.1", "2.2.2.2"] });
    render(<TestComponent />);
    expect(screen.getByTestId("ips").textContent).toBe(JSON.stringify(["1.1.1.1", "2.2.2.2"]));
  });

  it("throws error if provided experiment value is invalid", () => {
    (useCurrentWorkspace as jest.Mock).mockReturnValue({ dataplaneGroupId: "g1" });
    (useListDataplaneGroups as jest.Mock).mockReturnValue([{ dataplane_group_id: "g1", name: "US" }]);
    (useExperiment as jest.Mock).mockReturnValue({ us: ["3.3.3.3", "4.4.4.4", null, undefined, ""] });
    render(
      <ErrorBoundary>
        <TestComponent />
      </ErrorBoundary>
    );
    expect(screen.getByTestId("error").textContent).toContain(
      "Invalid experiment value for connector.airbyteCloudIpAddressesByDataplane"
    );
  });

  it("returns empty ip list if case if auto is not present in overwritten experiment value", () => {
    (useCurrentWorkspace as jest.Mock).mockReturnValue({ dataplaneGroupId: "g1" });
    (useListDataplaneGroups as jest.Mock).mockReturnValue([{ dataplane_group_id: "g1", name: "US" }]);
    (useExperiment as jest.Mock).mockReturnValue({ eu: ["5.5.5.5", "6.6.6.6"] });
    render(<TestComponent />);
    expect(screen.getByTestId("ips").textContent).toBe("");
  });

  it("returns IPs for a matching dataplane group (US)", () => {
    (useCurrentWorkspace as jest.Mock).mockReturnValue({ dataplaneGroupId: "g1" });
    (useListDataplaneGroups as jest.Mock).mockReturnValue([{ dataplane_group_id: "g1", name: "US" }]);
    (useExperiment as jest.Mock).mockReturnValue({ us: ["3.3.3.3", "4.4.4.4"] });
    render(<TestComponent />);
    expect(screen.getByTestId("ips").textContent).toBe(JSON.stringify(["3.3.3.3", "4.4.4.4"]));
  });

  it("returns IPs for a matching dataplane group (EU)", () => {
    (useCurrentWorkspace as jest.Mock).mockReturnValue({ dataplaneGroupId: "g2" });
    (useListDataplaneGroups as jest.Mock).mockReturnValue([{ dataplane_group_id: "g2", name: "EU" }]);
    (useExperiment as jest.Mock).mockReturnValue({ eu: ["5.5.5.5", "6.6.6.6"] });
    render(<TestComponent />);
    expect(screen.getByTestId("ips").textContent).toBe(JSON.stringify(["5.5.5.5", "6.6.6.6"]));
  });
});
