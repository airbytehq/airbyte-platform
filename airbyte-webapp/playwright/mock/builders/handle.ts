/**
 * A handle is an immutable description of one entity in a scenario. Other builders reference it by object,
 * not by list index or ID string, so TypeScript checks every reference. A test looks the entity up with the
 * same handle: `airbyte.store.connections.get(pokeConnection)`.
 *
 * The store assigns the ID when it builds a scenario, in the order it meets the handles.
 *
 * - `with(patch)` returns a variant of the SAME entity (same key, so the same ID).
 * - `copy(patch)` returns a NEW entity (new key, so the next ID).
 */
export interface Handle<Kind extends string, Input> {
  readonly kind: Kind;
  readonly key: string;
  readonly input: Readonly<Input>;
  with(patch: Partial<Input>): Handle<Kind, Input>;
  copy(patch?: Partial<Input>): Handle<Kind, Input>;
}

let handleCount = 0;

export function createHandle<Kind extends string, Input extends object>(
  kind: Kind,
  input: Input,
  key = `${kind}#${++handleCount}`
): Handle<Kind, Input> {
  return Object.freeze({
    kind,
    key,
    input: Object.freeze({ ...input }),
    with: (patch: Partial<Input>) => createHandle(kind, { ...input, ...patch }, key),
    copy: (patch: Partial<Input> = {}) => createHandle(kind, { ...input, ...patch }),
  });
}
