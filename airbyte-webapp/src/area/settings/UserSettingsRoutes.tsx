import React from "react";
import { Navigate, Route, Routes } from "react-router-dom";

import { CloudSettingsRoutePaths } from "packages/cloud/views/settings/routePaths";
import { SettingsRoutePaths } from "pages/routePaths";

import { UserSettingsLayout } from "./components/UserSettingsLayout";

const AccountSettingsView = React.lazy(() =>
  import("packages/cloud/views/users/AccountSettingsView/AccountSettingsView").then((module) => ({
    default: module.AccountSettingsView,
  }))
);
const ApplicationsView = React.lazy(() =>
  import("packages/cloud/views/users/ApplicationSettingsView/ApplicationSettingsView").then((module) => ({
    default: module.ApplicationSettingsView,
  }))
);
const AdvancedSettingsPage = React.lazy(() =>
  import("pages/SettingsPage/pages/AdvancedSettingsPage/AdvancedSettingsPage").then((module) => ({
    default: module.AdvancedSettingsPage,
  }))
);

export const UserSettingsRoutes: React.FC = () => {
  return (
    <Routes>
      <Route element={<UserSettingsLayout />}>
        <Route path={SettingsRoutePaths.Account} element={<AccountSettingsView />} />
        <Route path={CloudSettingsRoutePaths.Applications} element={<ApplicationsView />} />
        <Route path={CloudSettingsRoutePaths.Advanced} element={<AdvancedSettingsPage />} />
        <Route path="*" element={<Navigate to={SettingsRoutePaths.Account} replace />} />
      </Route>
    </Routes>
  );
};
