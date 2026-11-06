import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { RegionCard } from "./RegionCard";
import { Region } from "../../types";

const region: Region = {
  regionId: "region-1",
  name: "us-east-1",
  isAirbyteManaged: false,
  dataplanes: [
    {
      dataplane_id: "dp-1",
      dataplane_name: "us-east-1-dp-0",
      dataplane_group_id: "region-1",
      organization_id: "org-1",
      status: "HEALTHY",
      last_heartbeat_timestamp: Date.now(),
    },
    {
      dataplane_id: "dp-2",
      dataplane_name: "us-east-1-dp-1",
      dataplane_group_id: "region-1",
      organization_id: "org-1",
      status: "UNHEALTHY",
      last_heartbeat_timestamp: 1,
    },
  ],
  workspaces: [{ workspaceId: "ws-1", name: "Marketing" }],
};

describe("RegionCard", () => {
  it("names the header after the region, its badge and its status counts", async () => {
    await render(<RegionCard region={region} workspacesStatus="ready" />);

    expect(screen.getByRole("button", { expanded: false })).toHaveAccessibleName("us-east-1 Self-managed 1 1");
  });

  it("lists the workspaces once they are loaded", async () => {
    await render(<RegionCard region={region} workspacesStatus="ready" />);
    await userEvent.click(screen.getByRole("button", { expanded: false }));
    await userEvent.click(screen.getByRole("button", { name: "Workspaces (1)" }));

    expect(screen.getByText("Marketing")).toBeInTheDocument();
  });

  it("shows a spinner and no count while the workspaces load", async () => {
    await render(<RegionCard region={{ ...region, workspaces: [] }} workspacesStatus="loading" />);
    await userEvent.click(screen.getByRole("button", { expanded: false }));
    await userEvent.click(screen.getByRole("button", { name: "Workspaces" }));

    expect(screen.getByTestId("workspaces-loading")).toBeInTheDocument();
    expect(screen.queryByText("No workspaces", { exact: false })).not.toBeInTheDocument();
  });

  it("shows an error instead of the empty state when the workspaces query failed", async () => {
    await render(<RegionCard region={{ ...region, workspaces: [] }} workspacesStatus="error" />);
    await userEvent.click(screen.getByRole("button", { expanded: false }));
    await userEvent.click(screen.getByRole("button", { name: "Workspaces" }));

    expect(screen.getByTestId("workspaces-error")).toBeInTheDocument();
  });
});
