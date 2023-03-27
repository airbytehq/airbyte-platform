import { SyncSchemaField, Path } from "core/domain/catalog";

export type IndexerType = null | "required" | "sourceDefined";

export const flatten = (fArr: SyncSchemaField[], arr: SyncSchemaField[] = []): SyncSchemaField[] =>
  fArr.reduce<SyncSchemaField[]>((acc, f) => {
    acc.push(f);

    if (f.fields?.length) {
      return flatten(f.fields, acc);
    }
    return acc;
  }, arr);

export const getPathType = (required: boolean, shouldDefine: boolean): IndexerType =>
  required ? (shouldDefine ? "required" : "sourceDefined") : null;

export const pathDisplayName = (path: Path): string => path.join(".");
