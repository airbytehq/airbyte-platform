import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, createSearchParams } from "react-router-dom";

import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import { JobHistoryToTimelineRedirect } from "./ConnectionsRoutes";

jest.mock("react-router-dom", () => ({
  ...jest.requireActual("react-router-dom"),
  useLocation: jest.fn(),
  useNavigate: jest.fn(),
}));

jest.mock("area/workspace/utils", () => ({
  useCurrentWorkspaceLink: jest.fn(),
}));

describe(`${JobHistoryToTimelineRedirect.name}`, () => {
  const mockUseLocation = jest.requireMock("react-router-dom").useLocation;
  const mockUseNavigate = jest.requireMock("react-router-dom").useNavigate;
  const mockUseCurrentWorkspaceLink = jest.requireMock("area/workspace/utils").useCurrentWorkspaceLink;

  const mockWorkspaceId = "abc-def";
  const mockConnectionId = "999999";
  const mockJobId = "123";
  const mockAttemptNumber = "1";

  beforeEach(() => {
    mockUseNavigate.mockReset();
    mockUseLocation.mockReturnValue({ hash: "", pathname: "" });
    mockUseCurrentWorkspaceLink.mockReturnValue(
      (path: string): string => `/${RoutePaths.Workspaces}/${mockWorkspaceId}${path}`
    );
  });

  it(`should render ConnectionTimelinePage from /${ConnectionRoutePaths.Timeline}`, () => {
    render(
      <MemoryRouter
        initialEntries={[
          `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`,
        ]}
      >
        <Routes>
          <Route
            path={`/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`}
            element={<div>ConnectionTimelinePage</div>}
          />
          <Route path="*" element={<JobHistoryToTimelineRedirect />} />
        </Routes>
      </MemoryRouter>
    );

    expect(screen.getByText("ConnectionTimelinePage")).toBeInTheDocument();
  });

  it(`should redirect from /${ConnectionRoutePaths.JobHistory} to /${ConnectionRoutePaths.Timeline} and render ConnectionTimelinePage`, () => {
    const navigate = jest.fn();
    mockUseNavigate.mockReturnValue(navigate);
    mockUseLocation.mockReturnValue({
      hash: "",
      pathname: `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
    });

    render(
      <MemoryRouter
        initialEntries={[
          `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
        ]}
      >
        <Routes>
          <Route
            path={`/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`}
            element={<div>ConnectionTimelinePage</div>}
          />
          <Route path="*" element={<JobHistoryToTimelineRedirect />} />
        </Routes>
      </MemoryRouter>
    );

    expect(navigate).toHaveBeenCalledWith(
      expect.stringContaining(
        `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`
      ),
      {
        replace: true,
      }
    );
  });

  it(`should parse URL hash from a /${ConnectionRoutePaths.JobHistory} link into search params to open logs on /${ConnectionRoutePaths.Timeline} including attempt number`, () => {
    mockUseLocation.mockReturnValue({
      hash: `#${mockJobId}::${mockAttemptNumber}`,
      pathname: `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
    });
    const navigate = jest.fn();
    mockUseNavigate.mockReturnValue(navigate);

    render(
      <MemoryRouter
        initialEntries={[
          `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
        ]}
      >
        <Routes>
          <Route
            path={`/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`}
            element={<div>ConnectionTimelinePage</div>}
          />
          <Route path="*" element={<JobHistoryToTimelineRedirect />} />
        </Routes>
      </MemoryRouter>
    );

    const expectedSearchParams = createSearchParams({
      jobId: `${mockJobId}`,
      attemptNumber: `${mockAttemptNumber}`,
      openLogs: "true",
    }).toString();
    const expectedPath = `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}?${expectedSearchParams}`;

    expect(navigate).toHaveBeenCalledWith(expectedPath, { replace: true });
  });

  it(`should parse URL hash without an attempt number from a /${ConnectionRoutePaths.JobHistory} link into search params to open logs on /${ConnectionRoutePaths.Timeline}`, () => {
    mockUseLocation.mockReturnValue({
      hash: `#${mockJobId}`,
      pathname: `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
    });
    const navigate = jest.fn();
    mockUseNavigate.mockReturnValue(navigate);

    render(
      <MemoryRouter
        initialEntries={[
          `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.JobHistory}`,
        ]}
      >
        <Routes>
          <Route
            path={`/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}`}
            element={<div>ConnectionTimelinePage</div>}
          />
          <Route path="*" element={<JobHistoryToTimelineRedirect />} />
        </Routes>
      </MemoryRouter>
    );

    const expectedSearchParams = createSearchParams({
      jobId: `${mockJobId}`,
      openLogs: "true",
    }).toString();
    const expectedPath = `/${RoutePaths.Workspaces}/${mockWorkspaceId}/${RoutePaths.Connections}/${mockConnectionId}/${ConnectionRoutePaths.Timeline}?${expectedSearchParams}`;

    expect(navigate).toHaveBeenCalledWith(expectedPath, { replace: true });
  });
});
