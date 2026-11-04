import { render, screen } from "@testing-library/react";
import { MemoryRouter, Outlet, Route, Routes } from "react-router-dom";

import { SettingsLink, settingsPagePath } from "./SettingsNavigation";

const SettingsLayout = ({ layoutPath }: { layoutPath: string }) => (
  <>
    <SettingsLink iconType="gear" name="Workspace" to={settingsPagePath(layoutPath, "workspace")} />
    <Outlet />
  </>
);

const UserSettingsRoutes = () => (
  <Routes>
    <Route element={<SettingsLayout layoutPath="user" />}>
      <Route path="account" element={null} />
    </Route>
  </Routes>
);

const renderAt = (url: string) =>
  render(
    <MemoryRouter initialEntries={[url]}>
      <Routes>
        <Route path="workspaces/:workspaceId/*">
          <Route path="settings/*" element={<SettingsLayout layoutPath="settings" />}>
            <Route path="account" element={null} />
          </Route>
          <Route path="user/*" element={<UserSettingsRoutes />} />
        </Route>
      </Routes>
    </MemoryRouter>
  );

describe(`${settingsPagePath.name}`, () => {
  it("links to a sibling page from a settings/* layout", () => {
    renderAt("/workspaces/ws-1/settings/account");

    expect(screen.getByRole("link", { name: "Workspace" })).toHaveAttribute(
      "href",
      "/workspaces/ws-1/settings/workspace"
    );
  });

  it("links to a sibling page from a pathless layout inside user/*", () => {
    renderAt("/workspaces/ws-1/user/account");

    expect(screen.getByRole("link", { name: "Workspace" })).toHaveAttribute("href", "/workspaces/ws-1/user/workspace");
  });
});
