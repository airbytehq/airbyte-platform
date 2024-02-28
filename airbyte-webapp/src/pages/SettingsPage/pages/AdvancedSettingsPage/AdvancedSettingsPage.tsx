import { useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
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
  const [attemptsCount, setAttemptsCount] = useLocalStorage("airbyte_attempts-count-in-list", false);

  return (
    <Card title={formatMessage({ id: "settings.advancedSettings.title" })}>
      <FlexContainer gap="xl" direction="column">
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
          checked={attemptsCount}
          onCheckedChange={(checked) => setAttemptsCount(checked)}
          label={formatMessage({ id: "settings.advancedSettings.attemptCount" })}
          description={formatMessage({ id: "settings.advancedSettings.attemptCountDescription" })}
        />
      </FlexContainer>
    </Card>
  );
};
