import classNames from "classnames";
import { useField } from "formik";
import React from "react";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";
import { Text } from "components/ui/Text";

import styles from "./SslSshSwitcher.module.scss";
import { useSshSslImprovements } from "../../useSshSslImprovements";

export const SshSslSwitcher: React.FC = () => {
  const { dbHostIsLocalhost } = useSshSslImprovements();
  const [{ value: sslModeValue }, , sslHelper] = useField<string | undefined>("connectionConfiguration.ssl_mode.mode");
  const [{ value: tunnelModeValue }, , tunnelHelper] = useField<string | undefined>(
    "connectionConfiguration.tunnel_method.tunnel_method"
  );
  return (
    <FlexContainer>
      <label
        htmlFor="ssl"
        className={classNames(styles.tile, {
          [styles["tile--selected"]]: tunnelModeValue === "NO_TUNNEL",
          [styles["tile--disabled"]]: dbHostIsLocalhost,
        })}
      >
        <FlexItem>
          <RadioButton
            id="ssl"
            name="ssh-ssl"
            value="ssl"
            disabled={dbHostIsLocalhost}
            checked={tunnelModeValue === "NO_TUNNEL"}
            onChange={() => {
              if (sslModeValue === "disable" || sslModeValue === "prefer" || sslModeValue === "allow") {
                sslHelper.setValue("require");
              }
              tunnelHelper.setValue("NO_TUNNEL");
            }}
          />
        </FlexItem>
        <div>
          <FlexContainer direction="column">
            <Text size="lg" color={dbHostIsLocalhost ? "grey" : undefined}>
              SSL require
            </Text>
            <Text color={dbHostIsLocalhost ? "grey" : undefined}>
              Connect to Postgres directly over the internet using an SSL-encrypted connection.
            </Text>
          </FlexContainer>
        </div>
      </label>

      <label
        htmlFor="tunnel"
        className={classNames(styles.tile, { [styles["tile--selected"]]: tunnelModeValue !== "NO_TUNNEL" })}
      >
        <FlexItem>
          <RadioButton
            id="tunnel"
            name="ssh-ssl"
            value="tunnel"
            checked={tunnelModeValue !== "NO_TUNNEL"}
            onChange={() => {
              if (sslModeValue === "require") {
                sslHelper.setValue("prefer");
              }
              if (tunnelModeValue === "NO_TUNNEL") {
                tunnelHelper.setValue("SSH_KEY_AUTH");
              }
            }}
          />
        </FlexItem>
        <div>
          <FlexContainer direction="column">
            <Text size="lg">Set up a tunnel</Text>
            <Text>Set up a tunnel using SSH to connect to Postgres on a host that's not reachable directly.</Text>
          </FlexContainer>
        </div>
      </label>
    </FlexContainer>
  );
};
