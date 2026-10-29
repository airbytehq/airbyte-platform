import { getSpecSections } from "./specSections";

// The groups and sections of the source-postgres spec, which declares no section checks
const postgresSpec = {
  groups: [
    { id: "db" },
    { id: "security", title: "Network & security" },
    { id: "auth" },
    { id: "sync", title: "Sync method" },
    { id: "advanced", title: "Advanced" },
  ],
  sections: [
    {
      id: "connection",
      order: 0,
      title: "Connection",
      groups: ["db", "security"],
      description: "Where your Postgres database lives.",
    },
    {
      id: "credentials",
      order: 1,
      title: "Credentials",
      groups: ["auth"],
      description: "The Postgres user Airbyte will read your data with.",
    },
    {
      id: "sync_method",
      order: 2,
      title: "Sync method",
      groups: ["sync"],
      description: "How Airbyte reads changes from your database.",
    },
    {
      id: "review",
      order: 3,
      title: "Review & set up",
      groups: ["advanced"],
      description: "Name your source and check the optional settings.",
    },
  ],
};

// The groups and sections of the source-s3 cloud spec (airbytehq/airbyte#90132)
const s3Spec = {
  groups: [
    { id: "bucket" },
    { id: "endpoint", title: "More options" },
    { id: "files" },
    { id: "start_date", title: "Start date" },
  ],
  sections: [
    {
      id: "bucket",
      title: "Connect bucket",
      order: 0,
      description: "Which bucket to read from, and how Airbyte gets access.",
      groups: ["bucket", "endpoint"],
      check: true,
    },
    {
      id: "files",
      title: "Choose files",
      order: 1,
      description: "How files are delivered, and which files become streams.",
      groups: ["files", "start_date"],
      check: true,
    },
    {
      id: "review",
      title: "Review & set up",
      order: 2,
      description: "Name your source and check the optional settings.",
      groups: [],
    },
  ],
};

const INVALID_SECTIONS = "connectorForm.error.invalidSections";

describe("getSpecSections", () => {
  it("returns undefined for a spec without sections", () => {
    expect(getSpecSections({ groups: postgresSpec.groups })).toBeUndefined();
    expect(getSpecSections(undefined)).toBeUndefined();
  });

  it("accepts the source-postgres sections", () => {
    expect(getSpecSections(postgresSpec)?.map((section) => section.id)).toEqual([
      "connection",
      "credentials",
      "sync_method",
      "review",
    ]);
  });

  it("accepts the source-s3 sections, including a section without groups", () => {
    const sections = getSpecSections({ ...s3Spec, sections: [...s3Spec.sections].reverse() });

    expect(sections?.map((section) => section.id)).toEqual(["bucket", "files", "review"]);
    expect(sections?.[0].check).toBe(true);
    expect(sections?.[2].groups).toEqual([]);
  });

  it("accepts the groups that the webapp adds without the spec declaring them", () => {
    const sections = getSpecSections({
      groups: [{ id: "db" }],
      sections: [
        { id: "connection", title: "Connection", groups: ["db"] },
        { id: "review", title: "Review & set up", groups: ["default", "advanced"] },
      ],
    });

    expect(sections?.map((section) => section.groups)).toEqual([["db"], ["default", "advanced"]]);
  });

  it("puts sections with an order first and keeps the list order for the others", () => {
    const sections = getSpecSections({
      groups: [{ id: "a" }, { id: "b" }, { id: "c" }],
      sections: [
        { id: "a", title: "A", groups: ["a"] },
        { id: "b", title: "B", order: 1, groups: ["b"] },
        { id: "c", title: "C", order: 0, groups: ["c"] },
      ],
    });

    expect(sections?.map((section) => section.id)).toEqual(["c", "b", "a"]);
  });

  it("rejects an empty or missing list of sections", () => {
    expect(() => getSpecSections({ ...postgresSpec, sections: [] })).toThrow(INVALID_SECTIONS);
    expect(() => getSpecSections({ ...postgresSpec, sections: null })).toThrow(INVALID_SECTIONS);
  });

  it("rejects a section without an id, a title or a list of groups", () => {
    expect(() => getSpecSections({ ...postgresSpec, sections: [{ title: "Connection", groups: ["db"] }] })).toThrow(
      INVALID_SECTIONS
    );
    expect(() => getSpecSections({ ...postgresSpec, sections: [{ id: "connection", groups: ["db"] }] })).toThrow(
      INVALID_SECTIONS
    );
    expect(() => getSpecSections({ ...postgresSpec, sections: [{ id: "connection", title: "Connection" }] })).toThrow(
      INVALID_SECTIONS
    );
  });

  it("rejects a section whose order is not a number or whose check is not a boolean", () => {
    expect(() =>
      getSpecSections({ ...postgresSpec, sections: [{ id: "a", title: "A", order: "first", groups: ["db"] }] })
    ).toThrow(INVALID_SECTIONS);
    expect(() =>
      getSpecSections({ ...postgresSpec, sections: [{ id: "a", title: "A", check: "yes", groups: ["db"] }] })
    ).toThrow(INVALID_SECTIONS);
  });

  it("rejects two sections with the same id", () => {
    expect(() =>
      getSpecSections({
        ...postgresSpec,
        sections: [
          { id: "connection", title: "Connection", groups: ["db"] },
          { id: "connection", title: "Credentials", groups: ["auth"] },
        ],
      })
    ).toThrow(INVALID_SECTIONS);
  });

  it("rejects a group that is in two sections", () => {
    expect(() =>
      getSpecSections({
        ...postgresSpec,
        sections: [
          { id: "connection", title: "Connection", groups: ["db"] },
          { id: "credentials", title: "Credentials", groups: ["db", "auth"] },
        ],
      })
    ).toThrow(INVALID_SECTIONS);
  });

  it("rejects a group that the spec does not declare", () => {
    expect(() =>
      getSpecSections({
        ...postgresSpec,
        sections: [{ id: "connection", title: "Connection", groups: ["db", "replication"] }],
      })
    ).toThrow(INVALID_SECTIONS);
    expect(() => getSpecSections({ sections: [{ id: "connection", title: "Connection", groups: ["db"] }] })).toThrow(
      INVALID_SECTIONS
    );
  });
});
