import { FormBaseItem, FormGroupItem, SectionDetails } from "core/form/types";

import { getConnectorConfigKeys, getSectionCheckValues } from "./sectionCheckValues";
import { ConnectorFormValues } from "../../types";

function item(path: string, group?: string): FormBaseItem {
  return {
    _type: "formItem",
    fieldKey: path.split(".").at(-1) ?? path,
    isRequired: true,
    group,
    path,
    type: "string",
  };
}

function object(path: string, properties: FormBaseItem[], group?: string): FormGroupItem {
  return {
    _type: "formGroup",
    fieldKey: path,
    isRequired: true,
    group,
    path,
    properties,
  };
}

describe("getConnectorConfigKeys", () => {
  it("returns the top-level connector config key of each block and skips other blocks", () => {
    const host = item("connectionConfiguration.host", "db");
    const tunnel = object("connectionConfiguration.tunnel_method", [
      item("connectionConfiguration.tunnel_method.host"),
    ]);
    const name = item("name");

    expect(getConnectorConfigKeys([host, tunnel, name])).toEqual(["host", "tunnel_method"]);
  });

  it("returns no keys for no blocks", () => {
    expect(getConnectorConfigKeys([])).toEqual([]);
  });
});

describe("getSectionCheckValues", () => {
  const checkedSections: SectionDetails[] = [
    { id: "connection", title: "Connection", order: 0, check: true, groups: ["db"] },
    { id: "credentials", title: "Credentials", order: 1, check: true, groups: ["auth"] },
    { id: "review", title: "Review", order: 2, groups: ["advanced"] },
  ];
  const blocksBySection = [
    [item("connectionConfiguration.host", "db")],
    [object("connectionConfiguration.credentials", [item("connectionConfiguration.credentials.password")], "auth")],
    [item("connectionConfiguration.params", "advanced"), item("name", "__name_group")],
  ];
  const values: ConnectorFormValues = {
    name: "Postgres",
    connectionConfiguration: {
      host: "db.example.com",
      credentials: { password: "secret" },
      params: "ssl=true",
    },
    resourceAllocation: {},
  };

  it("holds the fields of the section and of the sections before it, and names the section", () => {
    expect(getSectionCheckValues(values, checkedSections, blocksBySection, 1)).toEqual({
      name: "Postgres",
      connectionConfiguration: {
        host: "db.example.com",
        credentials: { password: "secret" },
        "check-section": "credentials",
      },
      resourceAllocation: {},
    });
  });

  it("leaves out the fields of later sections", () => {
    expect(getSectionCheckValues(values, checkedSections, blocksBySection, 0).connectionConfiguration).toEqual({
      host: "db.example.com",
      "check-section": "connection",
    });
  });

  it("keeps fields that no section holds", () => {
    const valuesWithUnknownField = {
      ...values,
      connectionConfiguration: { ...values.connectionConfiguration, unknown: "value" },
    };

    expect(
      getSectionCheckValues(valuesWithUnknownField, checkedSections, blocksBySection, 0).connectionConfiguration
    ).toEqual({ host: "db.example.com", unknown: "value", "check-section": "connection" });
  });
});
