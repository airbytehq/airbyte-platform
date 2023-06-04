import React, {useContext} from "react";
import { FormattedMessage } from "react-intl";
import { HeadTitle } from "components/common/HeadTitle";
import AppearanceMode from "./components/AppearanceMode";
import { Content, SettingsCard } from "../SettingsComponents";
import {darkModeContext} from "../../../../App";


const AppearancePage: React.FC = () => {
    const { inDarkMode, setInDarkMode } = useContext(darkModeContext);

  const onToggleDarkModeButton = () => {
      setInDarkMode(!!!inDarkMode);
  };

  return (
    <>
      <HeadTitle titles={[{ id: "sidebar.settings" }, { id: "settings.appearance" }]} />
      <SettingsCard title={<FormattedMessage id="settings.appearanceSettings" />}>
        <Content>
          <AppearanceMode
            onChange={onToggleDarkModeButton}
          />
        </Content>
      </SettingsCard>
    </>
  );
};

export default AppearancePage;
