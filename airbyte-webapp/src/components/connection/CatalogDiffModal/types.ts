import { FieldTransform, StreamTransform } from "core/api/types/AirbyteClient";

export type DiffVerb = "new" | "removed" | "changed";

export interface SortedDiff<T extends StreamTransform | FieldTransform> {
  newItems: T[];
  removedItems: T[];
  changedItems: T[];
}
