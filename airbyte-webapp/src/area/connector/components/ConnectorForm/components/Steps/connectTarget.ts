import { FormBlock } from "core/form/types";

import { getConnectorConfigKeys } from "./sectionCheckValues";
import { ConnectTarget, ConnectorFormValues } from "../../types";

const TUNNEL_METHOD_KEY = "tunnel_method";
const NO_TUNNEL = "NO_TUNNEL";

const holdsHostAndPort = (blocks: FormBlock[]) => {
  const configKeys = getConnectorConfigKeys(blocks);
  return configKeys.includes("host") && configKeys.includes("port");
};

const usesTunnel = (connectionConfiguration: ConnectorFormValues["connectionConfiguration"]) => {
  const tunnel = connectionConfiguration[TUNNEL_METHOD_KEY] as { tunnel_method?: unknown } | undefined;
  const tunnelMethod = tunnel?.tunnel_method;
  return tunnelMethod !== undefined && tunnelMethod !== NO_TUNNEL;
};

/**
 * The host and port to test with the Connect command when the user completes a section. Undefined when the section must
 * use the section check: a later section such as credentials needs the connector, and behind an SSH tunnel a direct
 * connection says nothing about whether the database is reachable.
 */
export function getConnectTarget(
  connectionConfiguration: ConnectorFormValues["connectionConfiguration"],
  sectionBlocks: FormBlock[]
): ConnectTarget | undefined {
  if (!holdsHostAndPort(sectionBlocks) || usesTunnel(connectionConfiguration)) {
    return undefined;
  }

  const { host, port } = connectionConfiguration;
  return typeof host === "string" && host.length > 0 && typeof port === "number" ? { host, port } : undefined;
}
