import { screen } from "@testing-library/react";

import { render } from "test-utils";

import { WorkspaceDataWorkerGraphTooltip } from "./WorkspaceDataWorkerGraphTooltip";

const workspaceName = "Analytics workspace";
const hourlyPayload = [{ value: 1.46, payload: { date: "2026-06-01T19:00:00Z", used: 1.46 } }];

describe(`${WorkspaceDataWorkerGraphTooltip.name}`, () => {
  it("renders nothing when inactive or missing a payload", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip active={false} payload={hourlyPayload} workspaceName={workspaceName} />
    );

    expect(screen.queryByText(workspaceName)).not.toBeInTheDocument();

    await render(<WorkspaceDataWorkerGraphTooltip active payload={[]} workspaceName={workspaceName} />);

    expect(screen.queryByText(workspaceName)).not.toBeInTheDocument();
  });

  it("renders only the exact hourly timestamp, workspace name, and rounded usage", async () => {
    expect(process.env.TZ).toBe("US/Pacific");

    await render(<WorkspaceDataWorkerGraphTooltip active payload={hourlyPayload} workspaceName={workspaceName} />);

    expect(screen.getByText("Mon, Jun 1, 12:00 PM")).toBeInTheDocument();
    expect(screen.getByText(workspaceName)).toBeInTheDocument();
    expect(screen.getByText("1.5")).toBeInTheDocument();
    expect(screen.queryByText("Data workers")).not.toBeInTheDocument();
    expect(screen.queryByText("Region max")).not.toBeInTheDocument();
    expect(screen.queryByText("Per-workspace max")).not.toBeInTheDocument();
  });

  it("renders zero usage", async () => {
    await render(
      <WorkspaceDataWorkerGraphTooltip
        active
        payload={[{ value: 0, payload: { date: "2026-06-01T19:00:00Z", used: 0 } }]}
        workspaceName={workspaceName}
      />
    );

    expect(screen.getByText("0")).toBeInTheDocument();
  });
});
