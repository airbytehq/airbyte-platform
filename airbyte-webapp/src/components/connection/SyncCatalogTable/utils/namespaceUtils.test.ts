import { Row } from "@tanstack/react-table";

import { isNamespaceRow, getNamespaceRowId } from "./namespaceUtils";
import { SyncCatalogUIModel } from "../SyncCatalogTable";

/**
 * isNamespaceRow function tests
 */
describe(`${isNamespaceRow.name}`, () => {
  it("should return true if row is a namespace row", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 0,
      original: { rowType: "namespace" },
      id: "1",
    } as Row<SyncCatalogUIModel>;

    expect(isNamespaceRow(row)).toBe(true);
  });

  it("should return false if row is not a namespace row", () => {
    const row: Row<SyncCatalogUIModel> = {
      depth: 1,
      original: { rowType: "stream" },
      id: "2",
    } as Row<SyncCatalogUIModel>;

    expect(isNamespaceRow(row)).toBe(false);
  });
});

/**
 * getNamespaceRowId function tests
 */
describe("getNamespaceRowId", () => {
  it(`${getNamespaceRowId.name}`, () => {
    const row: Row<SyncCatalogUIModel> = {
      id: "namespace1.stream1",
    } as Row<SyncCatalogUIModel>;

    expect(getNamespaceRowId(row)).toBe("namespace1");
  });
});
