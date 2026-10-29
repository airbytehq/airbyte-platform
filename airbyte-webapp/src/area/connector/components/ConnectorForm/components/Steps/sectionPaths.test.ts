import { FormBaseItem, FormGroupItem, SectionDetails } from "core/form/types";

import { getErrorPaths, getSectionPaths, isPathInSection, isSameOrChildPath, splitSections } from "./sectionPaths";

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

const sections: SectionDetails[] = [
  { id: "connection", title: "Connection", order: 0, groups: ["db", "default"] },
  { id: "review", title: "Review", order: 1, groups: ["advanced"] },
];

describe("splitSections", () => {
  it("puts each block in the section that lists its group", () => {
    const host = item("connectionConfiguration.host", "db");
    const params = item("connectionConfiguration.params", "advanced");

    expect(splitSections([host, params], sections)).toEqual([[host], [params]]);
  });

  it("puts ungrouped blocks in the section that lists the default group", () => {
    const ungrouped = item("connectionConfiguration.ungrouped");

    expect(splitSections([ungrouped], sections)).toEqual([[ungrouped], []]);
  });

  it("puts blocks of groups that no section lists in the last section", () => {
    const name = item("name", "__name_group");
    const other = item("connectionConfiguration.other", "unlisted");

    expect(splitSections([name, other], sections)).toEqual([[], [name, other]]);
  });

  it("returns empty sections when there are no blocks", () => {
    expect(splitSections([], sections)).toEqual([[], []]);
  });
});

describe("getSectionPaths", () => {
  it("leaves out an object without properties, which only holds the OAuth button", () => {
    const oauthPlaceholder = object("connectionConfiguration", []);
    const credentials = object("connectionConfiguration.credentials", [
      item("connectionConfiguration.credentials.key"),
    ]);

    expect(getSectionPaths([oauthPlaceholder, credentials])).toEqual(["connectionConfiguration.credentials"]);
  });
});

describe("getErrorPaths", () => {
  it("returns the path of every field error in a nested error tree", () => {
    const errors = {
      name: { type: "required", message: "Required" },
      connectionConfiguration: {
        host: { type: "required", message: "Required" },
        replication_method: { publication: { type: "required", message: "Required" } },
      },
    };

    expect(getErrorPaths(errors, "")).toEqual([
      "name",
      "connectionConfiguration.host",
      "connectionConfiguration.replication_method.publication",
    ]);
  });

  it("returns the given path for a single field error", () => {
    expect(getErrorPaths({ type: "required" }, "connectionConfiguration.host")).toEqual([
      "connectionConfiguration.host",
    ]);
  });

  it("returns no paths when there are no errors", () => {
    expect(getErrorPaths(undefined, "connectionConfiguration.host")).toEqual([]);
    expect(getErrorPaths({}, "")).toEqual([]);
  });
});

describe("isSameOrChildPath", () => {
  it("matches the parent path and the paths nested under it, but not a path that only shares a prefix", () => {
    expect(isSameOrChildPath("connectionConfiguration.host", "connectionConfiguration.host")).toBe(true);
    expect(isSameOrChildPath("connectionConfiguration.host", "connectionConfiguration.host.nested")).toBe(true);
    expect(isSameOrChildPath("connectionConfiguration.host", "connectionConfiguration.hostname")).toBe(false);
    expect(isSameOrChildPath("connectionConfiguration.host", "connectionConfiguration")).toBe(false);
  });
});

describe("isPathInSection", () => {
  it("matches a section path and the paths nested under it, but not a path that only shares a prefix", () => {
    const sectionPaths = ["connectionConfiguration.host"];

    expect(isPathInSection(sectionPaths, "connectionConfiguration.host")).toBe(true);
    expect(isPathInSection(sectionPaths, "connectionConfiguration.host.nested")).toBe(true);
    expect(isPathInSection(sectionPaths, "connectionConfiguration.hostname")).toBe(false);
  });
});
