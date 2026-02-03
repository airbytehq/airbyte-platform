/* ---------------------------------------------------------------------------------------------
 *  Copyright (c) Microsoft Corporation. All rights reserved.
 *  Licensed under the MIT License. See License.txt in the project root for license information.
 *--------------------------------------------------------------------------------------------*/

// based on https://github.com/microsoft/monaco-languages/blob/master/src/twig/twig.ts

import { languages } from "monaco-editor";

import IRichLanguageConfiguration = languages.LanguageConfiguration;
import ILanguage = languages.IMonarchLanguage;

export const conf: IRichLanguageConfiguration = {
  wordPattern: /((\|\s*)?\w+(\(\))?)|(\| ?)/g,

  comments: {
    blockComment: ["{#", "#}"],
  },

  brackets: [
    ["{#", "#}"],
    ["{%", "%}"],
    ["{{", "}}"],
    ["(", ")"],
    ["[", "]"],

    // HTML
    ["<!--", "-->"],
    ["<", ">"],
  ],

  autoClosingPairs: [
    { open: "{# ", close: " #}" },
    { open: "{% ", close: " %}" },
    { open: "{{ ", close: " }}" },
    { open: "[", close: "]" },
    { open: "(", close: ")" },
    { open: '"', close: '"' },
    { open: "'", close: "'" },
  ],

  surroundingPairs: [
    { open: '"', close: '"' },
    { open: "'", close: "'" },

    // HTML
    { open: "<", close: ">" },
  ],
};

export const JINJA_TOKEN = "jinja";
export const NON_JINJA_TOKEN = "non-jinja";
export const JINJA_STRING_TOKEN = "jinja.string";
export const JINJA_OTHER_TOKEN = "jinja.other";
export const JINJA_FIRST_BRACKET_TOKEN = "jinja.bracket.first";
export const JINJA_CLOSING_BRACKET_TOKEN = "jinja.bracket.closing";

export const language: ILanguage = {
  tokenPostfix: "",
  ignoreCase: true,

  tokenizer: {
    root: [
      // Match the first `{` of `{{`
      [/\{/, { token: JINJA_FIRST_BRACKET_TOKEN, next: "@expectSecondBracket" }],
      [/[^{}]+|\{[^{}]*\}/, NON_JINJA_TOKEN], // Match text outside `{{ }}`
    ],
    expectSecondBracket: [
      // Match the second `{` and transition into `insideBrackets`
      [/\{/, { token: JINJA_TOKEN, next: "@insideBrackets" }],
      // Fallback for invalid syntax (single `{` without a second `{`)
      [/./, { token: NON_JINJA_TOKEN, next: "@pop" }],
    ],
    insideBrackets: [
      // Exit `{{ }}`
      [/\}\}/, { token: JINJA_CLOSING_BRACKET_TOKEN, next: "@pop" }],
      // Enter `""` inside `{{ }}`
      [/["']/, { token: JINJA_STRING_TOKEN, next: "@insideString" }],
      // Match alphanumeric and whitespace
      [/[\w\s(|]/, JINJA_TOKEN],
      // Match other characters
      [/[^\w\s(|]/, JINJA_OTHER_TOKEN],
    ],
    insideString: [
      // Exit `""` and return to `@insideBrackets`
      [/["']/, { token: JINJA_STRING_TOKEN, next: "@pop" }],
      // Match string content inside `""`
      [/[^"']+/, JINJA_STRING_TOKEN],
    ],
  },
};
