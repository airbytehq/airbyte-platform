import { setIn, useFormikContext } from "formik";
import { JSONSchema7Type } from "json-schema";
import get from "lodash/get";
import { useEffect } from "react";

import { FormGroupItem } from "core/form/types";
import { useExperiment } from "hooks/services/Experiment";
import { ConnectorIds } from "utils/connectors";

import { useConnectorForm } from "./connectorFormContext";
import { ConnectorFormValues } from "./types";
import { isLocalhost } from "./utils";

const tunnelModePath = "connectionConfiguration.tunnel_method.tunnel_method";
const sslModePath = "connectionConfiguration.ssl_mode.mode";

export const useSshSslImprovements = (path?: string) => {
  const showSshSslExperiment = useExperiment("connector.form.sshSslImprovements", false);
  const showSimplifiedConfiguration = useExperiment("connector.form.simplifyConfiguration", false);
  const { selectedConnectorDefinition } = useConnectorForm();
  const { values, setValues } = useFormikContext<ConnectorFormValues>();
  const hostValue = get(values, "connectionConfiguration.host");
  const tunnelModeValue = get(values, tunnelModePath);
  const sslModeValue = get(values, sslModePath);

  const isTunnelMethod = path === "connectionConfiguration.tunnel_method";
  const isSSLMode = path === "connectionConfiguration.ssl_mode";

  const showSshSslImprovements =
    showSshSslExperiment &&
    showSimplifiedConfiguration &&
    selectedConnectorDefinition !== undefined &&
    "sourceDefinitionId" in selectedConnectorDefinition &&
    selectedConnectorDefinition.sourceDefinitionId === ConnectorIds.Sources.Postgres;

  const dbHostIsLocalhost = isLocalhost(hostValue);
  const securityMode = tunnelModeValue !== "NO_TUNNEL" ? ("tunnel" as const) : ("ssl" as const);

  // Automatically select tunnel mode if db host is localhost - behave as if the user clicked the "set up a tunnel" tile
  useEffect(() => {
    if (showSshSslImprovements && dbHostIsLocalhost && isTunnelMethod && tunnelModeValue === "NO_TUNNEL") {
      let newFormValues = setIn(values, tunnelModePath, "SSH_KEY_AUTH");
      if (sslModeValue === "require") {
        newFormValues = setIn(newFormValues, sslModePath, "prefer");
      }
      setValues(newFormValues);
    }
  }, [dbHostIsLocalhost, isTunnelMethod, setValues, showSshSslImprovements, sslModeValue, tunnelModeValue, values]);

  return {
    showSshSslImprovements,
    dbHostIsLocalhost,
    filterConditions: (conditions: FormGroupItem[]) =>
      conditions.filter((condition) => {
        if (!showSshSslImprovements) {
          return true;
        }
        if (dbHostIsLocalhost && isTunnelMethod) {
          // If db host is localhost, we need a tunnel
          return condition.title !== "No Tunnel";
        }
        if (securityMode === "tunnel" && isTunnelMethod) {
          // If tunnel is set, do not allow to switch to no tunnel (SslSshSwitcher component takes care of that)
          return condition.title !== "No Tunnel";
        }
        if (isSSLMode && securityMode === "ssl") {
          // If no tunnel is set, do not allow to switch to unsecure ssl modes
          return condition.title !== "disable" && condition.title !== "prefer" && condition.title !== "allow";
        }
        return true;
      }),
    filterSelectionConstValues: (constValues: JSONSchema7Type[]) =>
      constValues.filter((constValue) => {
        if (!showSshSslImprovements) {
          return true;
        }
        if (dbHostIsLocalhost && isTunnelMethod) {
          // If db host is localhost, we need a tunnel
          return constValue !== "NO_TUNNEL";
        }
        if (securityMode === "tunnel" && isTunnelMethod) {
          // If tunnel is set, do not allow to switch to no tunnel (SslSshSwitcher component takes care of that)
          return constValue !== "NO_TUNNEL";
        }
        if (isSSLMode && securityMode === "ssl") {
          // If no tunnel is set, do not allow to switch to unsecure ssl modes
          return constValue !== "disable" && constValue !== "prefer" && constValue !== "allow";
        }
        return true;
      }),
  };
};
