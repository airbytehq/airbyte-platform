declare module "any-base" {
  interface AnyBase {
    (from: string, to: string): (input: string) => string;
    HEX: string;
  }
  declare const anyBase: AnyBase;
  export = anyBase;
}
