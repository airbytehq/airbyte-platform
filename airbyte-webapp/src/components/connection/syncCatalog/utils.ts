import { SyncSchemaField, Path } from "core/domain/catalog";

export type FieldPathType = null | "required" | "sourceDefined";

export const flattenSyncSchemaFields = (fArr: SyncSchemaField[], arr: SyncSchemaField[] = []): SyncSchemaField[] =>
  fArr.reduce<SyncSchemaField[]>((acc, f) => {
    acc.push(f);

    if (f.fields?.length) {
      return flattenSyncSchemaFields(f.fields, acc);
    }
    return acc;
  }, arr);

export const getFieldPathType = (required: boolean, shouldDefine: boolean): FieldPathType =>
  required ? (shouldDefine ? "required" : "sourceDefined") : null;

export const getFieldPathDisplayName = (path: Path): string => path.join(".");
