import { useIntl } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import styles from "./DisabledStreamsSwitch.module.scss";

interface DisabledStreamsSwitchProps {
  checked: boolean;
  onChange: () => void;
}

export const DisabledStreamsSwitch: React.FC<DisabledStreamsSwitchProps> = ({ checked, onChange }) => {
  const { formatMessage } = useIntl();
  return (
    <FlexContainer alignItems="center" className={styles.label} gap="sm">
      <Switch
        size="sm"
        checked={checked}
        onChange={onChange}
        data-testid="hideDisableStreams-switch"
        id="toggle-disabled-streams"
      />
      <label htmlFor="toggle-disabled-streams">
        <Text color="grey">{formatMessage({ id: "form.hideDisabledStreams" })}</Text>
      </label>
    </FlexContainer>
  );
};
