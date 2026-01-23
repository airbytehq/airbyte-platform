import { TextDecoder, TextEncoder } from "util";

import JSDOMEnvironment from "jest-environment-jsdom";

// https://github.com/jsdom/jsdom/issues/3363#issuecomment-1921575184 required for structuredClone which is used in our catalog helpers

// https://github.com/facebook/jest/blob/v29.4.3/website/versioned_docs/version-29.4/Configuration.md#testenvironment-string
export default class FixJSDOMEnvironment extends JSDOMEnvironment {
  constructor(...args: ConstructorParameters<typeof JSDOMEnvironment>) {
    super(...args);

    // FIXME https://github.com/jsdom/jsdom/issues/3363
    this.global.structuredClone = structuredClone;

    // Polyfill TextEncoder/TextDecoder for packages that use Web Encoding API
    // (e.g., @melloware/react-logviewer)
    // Node.js types differ slightly from browser types, so we need to cast
    this.global.TextEncoder = TextEncoder as typeof global.TextEncoder;
    this.global.TextDecoder = TextDecoder as typeof global.TextDecoder;
  }
}
