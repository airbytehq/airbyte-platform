import classNames from "classnames";
import MarkdownToJsx from "markdown-to-jsx";
import React, { useMemo } from "react";
import { remark } from "remark";
import remarkGfm from "remark-gfm";

import styles from "./Markdown.module.scss";
import { Admonition, Details, DocTabs, Pre, HideInUI } from "./overrides";

type Options = Parameters<typeof MarkdownToJsx>[0]["options"];

interface MarkdownToJsxProps {
  className?: string;
  content: string;
  options?: Options;
}

function surroundTagWithNewlines(tag: string, markdown: string): string {
  const processed = markdown
    .replace(new RegExp(`([^\n])\n<${tag}>`, "g"), `$1\n\n<${tag}>`)
    .replace(new RegExp(`</${tag}>\n([^\n])`, "g"), `</${tag}>\n\n$1`);

  return processed;
}

function preprocessMarkdown(markdown: string): string {
  // Note: there is also some preprocessing happening in DocumentationPanel.tsx's
  // prepareMarkdown function that is specific to the connector documentation pages.

  // Remove frontmatter (content wrapped in ---) from the beginning of the markdown.
  let preprocessed = markdown.replace(/^\s*---\n[\s\S]*?\n---\n/, "");

  // remove any ESM-style import statements from the markdown: Docusaurus implements its
  // Tabs feature with MDX components, which must be imported.
  preprocessed = preprocessed.replace(/^(?:\n|import (?:{ ?)?[^ ]*(?: ?})? from [^ ]*\n)*/, "");

  // Replace docusaurus-style admonitions with custom admonition component.
  preprocessed = preprocessed.replace(
    /:::(info|warning|note|tip|caution|danger)\s*\n([\s\S]*?)\n\s*:::\s*\n/g,
    '<admonition type="$1">$2</admonition>\n'
  );

  // Add a zero-width space character before every character that looks like the start of a
  // list item, but actually isn't because it's not preceded by only whitespace characters.
  // The reason is to prevent markdown-to-jsx from rendering them as separate sub-bullets
  // as a result of the following linked issue. Once resolved, this can be removed:
  // https://github.com/probablyup/markdown-to-jsx/issues/285#issuecomment-1813224435
  //
  // Since hyphens have a few context-dependent meanings in markdown documents outside of
  // bullet lists, like html comments and gfm table syntax, we need a somewhat complex set
  // of lookbehind assertions: besides legitimate lists, we don't want to insert a
  // zero-width space into a table definition or html comment and break their rendering,
  // either.
  preprocessed = preprocessed.replace(/(?<!<!-|-|:|\|)(?<=\S)(- |\+ |[^*]\* |\d+\. )/g, "&ZeroWidthSpace;$1");

  // Ensure there's an empty line before and after tags with custom react logic if there
  // isn't one already, to ensure that it is parsed as its own component.
  // The motivation for this is that Docusaurus renders these properly even if
  // there aren't empty lines surrounding it, so without this preprocessing step, a
  // documentation-site-only review of content changes could hide an in-app error state.
  //
  // This should be the penultimate step, before remarkGfm is run, to ensure all instances
  // are caught.
  preprocessed = surroundTagWithNewlines("details", preprocessed);
  // And likewise for <Tabs>
  preprocessed = surroundTagWithNewlines("Tabs", preprocessed);

  // Apply remark plugins to the markdown.
  // This should be ran last so that remarkGfm doesn't interfere with the above.
  preprocessed = remark().use(remarkGfm).processSync(preprocessed).toString();

  return preprocessed;
}

export const Markdown: React.FC<MarkdownToJsxProps> = React.memo(({ className, content, options }) => {
  const processedMarkdown = useMemo(() => preprocessMarkdown(content), [content]);
  return (
    <div className={classNames(className, styles.markdown)}>
      <MarkdownToJsx
        options={{
          ...options,
          overrides: {
            ...options?.overrides,
            pre: {
              component: Pre,
            },
            details: {
              component: Details,
            },
            admonition: {
              component: Admonition,
            },
            HideInUI: {
              component: HideInUI,
            },
            Tabs: {
              component: DocTabs,
            },
          },
        }}
      >
        {processedMarkdown}
      </MarkdownToJsx>
    </div>
  );
});

Markdown.displayName = "Markdown";
