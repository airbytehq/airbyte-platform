import { FormGroupItem, FormBaseItem, FormConditionItem, FormBlock, GroupDetails } from "core/form/types";

import { DisplayType, generateGroupsAndSections, Section } from "./useGroupsAndSections";
import { getFieldPath, hasCollapsibleDefault } from "../../utils";

function object(
  items: FormBlock[],
  name = "unnamed group",
  { required, order, group }: { required?: boolean; order?: number; group?: string } = {
    required: false,
    order: undefined,
    group: undefined,
  }
): FormGroupItem {
  return {
    _type: "formGroup",
    fieldKey: name,
    isRequired: Boolean(required),
    order,
    group,
    path: name,
    properties: items,
  };
}

function item(
  name: string,
  {
    required,
    order,
    group,
    hidden,
    always_show,
    default: defaultValue,
    const: constValue,
    enum: enumValues,
  }: {
    required?: boolean;
    order?: number;
    group?: string;
    hidden?: boolean;
    always_show?: boolean;
    default?: FormBaseItem["default"];
    const?: FormBaseItem["const"];
    enum?: FormBaseItem["enum"];
  } = {
    required: false,
    order: undefined,
    group: undefined,
  }
): FormBaseItem {
  return {
    _type: "formItem",
    fieldKey: name,
    isRequired: Boolean(required),
    order,
    group,
    path: name,
    airbyte_hidden: hidden,
    always_show,
    default: defaultValue,
    const: constValue,
    enum: enumValues,
    type: "string",
  };
}

function condition(name: string, defaultValue: string): FormConditionItem {
  return {
    _type: "formCondition",
    fieldKey: name,
    isRequired: true,
    path: name,
    default: defaultValue,
    conditions: [object([item("api_key", { required: true })], "api")],
    selectionKey: "authType",
    selectionConstValues: [defaultValue],
  };
}

function sectionGroup(sections: Section[], title?: string) {
  return {
    sections,
    title,
  };
}

function section(blocks: FormBlock[], displayType: DisplayType = "expanded"): Section {
  return {
    blocks,
    displayType,
  };
}

const isHiddenField = jest.fn(() => false);

function generate(blocks: FormBlock | FormBlock[], groups: GroupDetails[] = [], rootLevel = true) {
  return generateGroupsAndSections(blocks, groups, rootLevel, isHiddenField);
}

describe("useGroupsAndSections", () => {
  it("should keep required fields with blank defaults expanded", () => {
    const emptyStringDefault = item("s3_bucket_region", {
      required: true,
      default: "",
      enum: ["", "us-east-1", "us-west-1"],
    });
    const nullDefault = item("required_with_null_default", { required: true, default: null });

    expect(generate([emptyStringDefault, nullDefault])).toEqual([
      sectionGroup([section([nullDefault, emptyStringDefault])]),
    ]);
  });

  it("should treat zero as a collapsible default", () => {
    expect(hasCollapsibleDefault(item("zero_default", { required: true, default: 0 }))).toBe(true);
  });

  it("should not collapse required fields whose default the user must still change", () => {
    expect(hasCollapsibleDefault(item("accept_terms", { required: true, default: false }))).toBe(false);
    expect(hasCollapsibleDefault(item("columns", { required: true, default: [] }))).toBe(false);
    expect(hasCollapsibleDefault({ ...item("api_key", { required: true, default: "key" }), isSecret: true })).toBe(
      false
    );
  });

  it("should resolve field paths with and without a path prefix", () => {
    const field = item("region");

    expect(getFieldPath(field)).toBe("region");
    expect(getFieldPath(field, "connectionConfiguration")).toBe("connectionConfiguration.region");
    expect(getFieldPath(field, "connectionConfiguration.auth", true)).toBe("connectionConfiguration.auth");
  });

  it("should put optional fields in the back", () => {
    expect(generate([item("a"), item("b"), item("c", { required: true })])).toEqual([
      sectionGroup([section([item("c", { required: true })]), section([item("a"), item("b")], "collapsed-footer")]),
    ]);
  });

  it("should order optional fields in the back but respect order within collapsed section", () => {
    expect(
      generate([
        item("a", { order: 1 }),
        item("b", { order: 0 }),
        item("c", { required: true, order: 2 }),
        item("d", { order: 3 }),
      ])
    ).toEqual([
      sectionGroup([
        section([item("c", { required: true, order: 2 })]),
        section([item("b", { order: 0 }), item("a", { order: 1 }), item("d", { order: 3 })], "collapsed-footer"),
      ]),
    ]);
  });

  it("should collapse required fields with defaults into the root footer", () => {
    const requiredField = item("required", { required: true });
    const defaultedRequiredField = item("required_with_default", { required: true, default: "safe-value" });

    expect(generate([defaultedRequiredField, requiredField])).toEqual([
      sectionGroup([section([requiredField]), section([defaultedRequiredField], "collapsed-footer")]),
    ]);
  });

  it("should collapse required fields with defaults inline when nested", () => {
    const requiredField = item("required", { required: true });
    const defaultedRequiredField = item("required_with_default", { required: true, default: "safe-value" });

    expect(generate([defaultedRequiredField, requiredField], [], false)).toEqual([
      sectionGroup([section([requiredField]), section([defaultedRequiredField], "collapsed-inline")]),
    ]);
  });

  it("should order required fields with defaults alongside optional fields when nested", () => {
    const requiredField = item("required", { required: true, order: 2 });
    const defaultedRequiredField = item("required_with_default", { required: true, default: "safe-value", order: 1 });
    const optionalField = item("optional", { order: 0 });

    expect(generate([defaultedRequiredField, requiredField, optionalField], [], false)).toEqual([
      sectionGroup([section([requiredField]), section([optionalField, defaultedRequiredField], "collapsed-inline")]),
    ]);
  });

  it("should keep required fields with defaults expanded when always_show is true", () => {
    const field = item("required_with_default", { required: true, default: "safe-value", always_show: true });

    expect(generate(field)).toEqual([sectionGroup([section([field])])]);
  });

  it("should keep required fields without defaults expanded", () => {
    const field = item("required_without_default", { required: true });

    expect(generate(field)).toEqual([sectionGroup([section([field])])]);
  });

  it("should keep required const fields with defaults expanded", () => {
    const field = item("required_const", { required: true, default: "fixed-value", const: "fixed-value" });

    expect(generate(field)).toEqual([sectionGroup([section([field])])]);
  });

  it("should keep required oneOf conditions with defaults expanded", () => {
    const authCondition = condition("auth", "api");

    expect(generate(authCondition)).toEqual([sectionGroup([section([authCondition])])]);
  });

  it("should order optional fields with 'always_show: true' normally", () => {
    expect(
      generate([
        item("a", { order: 1 }),
        item("b", { order: 0, always_show: true }),
        item("c", { required: true, order: 2 }),
        item("d", { order: 3 }),
      ])
    ).toEqual([
      sectionGroup([
        section([item("b", { order: 0, always_show: true }), item("c", { required: true, order: 2 })]),
        section([item("a", { order: 1 }), item("d", { order: 3 })], "collapsed-footer"),
      ]),
    ]);
  });

  it("should not unwrap nested objects", () => {
    expect(
      generate([
        item("a", { order: 0 }),
        object([item("b", { order: 0 }), item("c", { required: true, order: 1 })], "group1", {
          required: true,
          order: 1,
        }),
        item("d", { order: 2 }),
      ])
    ).toEqual([
      sectionGroup([
        section(
          [
            object([item("b", { order: 0 }), item("c", { required: true, order: 1 })], "group1", {
              required: true,
              order: 1,
            }),
          ],
          "expanded"
        ),
        section([item("a", { order: 0 }), item("d", { order: 2 })], "collapsed-footer"),
      ]),
    ]);
  });

  it("should split up groups and order and collapse them individually", () => {
    expect(
      generate([
        item("a", { group: "b", order: 1 }),
        item("b", { group: "b", order: 0 }),
        item("c", { required: true, group: "a" }),
        item("d", { group: "a" }),
      ])
    ).toEqual([
      sectionGroup([
        section([item("c", { required: true, group: "a" })]),
        section([item("d", { group: "a" })], "collapsed-footer"),
      ]),
      sectionGroup([
        section([item("b", { group: "b", order: 0 }), item("a", { group: "b", order: 1 })], "collapsed-group"),
      ]),
    ]);
  });

  it("should not create a group if the fields in this group are hidden", () => {
    expect(generate([item("x", { required: true, group: "a" }), item("hidden", { group: "b", hidden: true })])).toEqual(
      [sectionGroup([section([item("x", { required: true, group: "a" })])])]
    );
  });

  it("should group nested objects as a whole and ignore group tags set on nested fields", () => {
    expect(
      generate([
        object([item("b", { group: "b", order: 0 }), item("c", { required: true, group: "c" })], "z_group", {
          required: true,
          group: "a",
          order: 2,
        }),
        item("d", { group: "b", order: 1 }),
        item("a", { required: true, group: "a", order: 1 }),
      ])
    ).toEqual([
      sectionGroup([
        section(
          [
            item("a", { required: true, group: "a", order: 1 }),
            object([item("b", { group: "b", order: 0 }), item("c", { required: true, group: "c" })], "z_group", {
              required: true,
              group: "a",
              order: 2,
            }),
          ],
          "expanded"
        ),
      ]),
      sectionGroup([section([item("d", { group: "b", order: 1 })], "collapsed-group")]),
    ]);
  });

  it("should title and order groups based on groups field", () => {
    expect(
      generate(
        [
          item("a", { group: "b", order: 1 }),
          item("b", { group: "b", order: 0 }),
          item("c", { required: true, group: "a" }),
          item("d", { group: "a" }),
          item("e"),
        ],
        [
          { id: "b", title: "Group B" },
          { id: "a", title: "Group A" },
        ]
      )
    ).toEqual([
      sectionGroup(
        [section([item("b", { group: "b", order: 0 }), item("a", { group: "b", order: 1 })], "collapsed-group")],
        "Group B"
      ),
      sectionGroup(
        [
          section([item("c", { required: true, group: "a" })]),
          section([item("d", { group: "a" })], "collapsed-footer"),
        ],
        "Group A"
      ),
      sectionGroup([section([item("e")], "collapsed-group")]),
    ]);
  });
});
