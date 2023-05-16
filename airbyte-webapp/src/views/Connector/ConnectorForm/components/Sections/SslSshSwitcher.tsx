import classNames from "classnames";
import React from "react";
import { useFormContext } from "react-hook-form";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { RadioButton } from "components/ui/RadioButton";
import { Text } from "components/ui/Text";

import styles from "./SslSshSwitcher.module.scss";
import { useSshSslImprovements } from "../../useSshSslImprovements";

export const SshSslSwitcher: React.FC = () => {
  const { dbHostIsLocalhost } = useSshSslImprovements();
  const { watch, setValue } = useFormContext();
  const sslModeValue = watch("connectionConfiguration.ssl_mode.mode");
  const tunnelModeValue = watch("connectionConfiguration.tunnel_method.tunnel_method");
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
                setValue("connectionConfiguration.ssl_mode.mode", "require");
              }
              setValue("connectionConfiguration.tunnel_method.tunnel_method", "NO_TUNNEL");
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
                setValue("connectionConfiguration.ssl_mode.mode", "prefer");
              }
              if (tunnelModeValue === "NO_TUNNEL") {
                setValue("connectionConfiguration.tunnel_method.tunnel_method", "SSH_KEY_AUTH");
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
