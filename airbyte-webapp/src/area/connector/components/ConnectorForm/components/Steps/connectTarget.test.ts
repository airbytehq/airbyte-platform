import { FormBaseItem, FormBlock } from "core/form/types";

import { getConnectTarget } from "./connectTarget";

function item(key: string): FormBaseItem {
  return {
    _type: "formItem",
    fieldKey: key,
    isRequired: true,
    path: `connectionConfiguration.${key}`,
    type: "string",
  };
}

const connectionBlocks: FormBlock[] = [item("host"), item("port"), item("database")];
const credentialsBlocks: FormBlock[] = [item("username"), item("password")];
const tunnelBlock = item("tunnel_method");

const hostAndPort = { host: "db.example.com", port: 5432 };

describe("getConnectTarget", () => {
  it("returns only the host and port of the values", () => {
    expect(
      getConnectTarget({ ...hostAndPort, database: "analytics", "check-section": "connection" }, connectionBlocks)
    ).toEqual(hostAndPort);
  });

  describe("the section's own fields", () => {
    it("returns no target for a later section, such as Credentials, when the values hold a host and port", () => {
      expect(getConnectTarget(hostAndPort, credentialsBlocks)).toBeUndefined();
    });

    it("returns no target when the section holds a host without a port", () => {
      expect(getConnectTarget(hostAndPort, [item("host"), item("database")])).toBeUndefined();
    });
  });

  describe("the values", () => {
    it.each([
      ["an empty host", { host: "", port: 5432 }],
      ["a string port", { host: "db.example.com", port: "5432" }],
    ])("returns no target for %s", (_description, connectionConfiguration) => {
      expect(getConnectTarget(connectionConfiguration, connectionBlocks)).toBeUndefined();
    });
  });

  describe("tunnel_method", () => {
    const blocks = [...connectionBlocks, tunnelBlock];

    it("returns the target when the spec has no tunnel_method", () => {
      expect(getConnectTarget(hostAndPort, connectionBlocks)).toEqual(hostAndPort);
    });

    it("returns the target for NO_TUNNEL", () => {
      const tunnelValues = { tunnel_method: { tunnel_method: "NO_TUNNEL" } };

      expect(getConnectTarget({ ...hostAndPort, ...tunnelValues }, blocks)).toEqual(hostAndPort);
    });

    it("returns no target for an SSH tunnel", () => {
      const tunnelValues = {
        tunnel_method: { tunnel_method: "SSH_KEY_AUTH", tunnel_host: "bastion.example.com", tunnel_port: 22 },
      };

      expect(getConnectTarget({ ...hostAndPort, ...tunnelValues }, blocks)).toBeUndefined();
    });
  });
});
