import { FormGroupItem, FormBaseItem, FormBlock, GroupDetails } from "core/form/types";

import { DisplayType, generateGroupsAndSections, Section } from "./useGroupsAndSections";

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
  }: { required?: boolean; order?: number; group?: string; hidden?: boolean; always_show?: boolean } = {
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
    type: "string",
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

const isHiddenAuthField = jest.fn(() => false);

function generate(blocks: FormBlock | FormBlock[], groups: GroupDetails[] = []) {
  return generateGroupsAndSections(blocks, groups, true, isHiddenAuthField);
}

describe("useGroupsAndSections", () => {
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
