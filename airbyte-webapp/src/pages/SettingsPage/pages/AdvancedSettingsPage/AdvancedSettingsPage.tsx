import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useLocalStorage } from "core/utils/useLocalStorage";

import styles from "./AdvancedSettingsPage.module.scss";

interface SwitchSettingProps {
  id: string;
  onCheckedChange: (value: boolean) => void;
  checked: boolean;
  label: string;
  description: string;
}

const SwitchSetting: React.FC<SwitchSettingProps> = ({ id, onCheckedChange, checked, description, label }) => {
  return (
    <div className={styles.switchSetting}>
      <Switch size="sm" checked={checked} onChange={(ev) => onCheckedChange(ev.target.checked)} id={id} />
      <Text bold as="div">
        <label htmlFor={id}>{label}</label>
      </Text>
      <Text as="div" className={styles.switchSetting__description}>
        {description}
      </Text>
    </div>
  );
};

export const AdvancedSettingsPage: React.FC = () => {
  const { formatMessage } = useIntl();
  const [workspaceInTitle, setWorkspaceInTitle] = useLocalStorage("airbyte_workspace-in-title", false);
  const [attemptsStats, setAttemptsStats] = useLocalStorage("airbyte_extended-attempts-stats", false);
  const [connectionDetails, setConnectionDetails] = useLocalStorage("airbyte_connection-additional-details", false);

  return (
    <FlexContainer gap="xl" direction="column">
      <Heading as="h1">{formatMessage({ id: "settings.advancedSettings.title" })}</Heading>
      <Message type="info" text={formatMessage({ id: "settings.advancedSettings.description" })} />
      <SwitchSetting
        id="workspace-in-title"
        checked={workspaceInTitle}
        onCheckedChange={(checked) => setWorkspaceInTitle(checked)}
        label={formatMessage({ id: "settings.advancedSettings.workspaceInTitle" })}
        description={formatMessage({ id: "settings.advancedSettings.workspaceInTitleDescription" })}
      />
      <SwitchSetting
        id="attempts-count-in-list"
        checked={attemptsStats}
        onCheckedChange={(checked) => setAttemptsStats(checked)}
        label={formatMessage({ id: "settings.advancedSettings.attemptStats" })}
        description={formatMessage({ id: "settings.advancedSettings.attemptStatsDescription" })}
      />
      <SwitchSetting
        id="connection-additional-details"
        checked={connectionDetails}
        onCheckedChange={(checked) => setConnectionDetails(checked)}
        label={formatMessage({ id: "settings.advancedSettings.connectionDetails" })}
        description={formatMessage({ id: "settings.advancedSettings.connectionDetailsDescription" })}
      />
    </FlexContainer>
  );
};
