import classNames from "classnames";
import MarkdownToJsx from "markdown-to-jsx";
import React, { ReactNode } from "react";
import { remark } from "remark";
import remarkGfm from "remark-gfm";

// Since we're dynamically accessing the admonition--{node.name} classes, the linter
// can't determine that those are used, thus we need to ignore unused classes here.
// eslint-disable-next-line css-modules/no-unused-class
import styles from "./Markdown.module.scss";
import { Collapsible } from "../Collapsible";
import { CopyButton } from "../CopyButton";

type Options = Parameters<typeof MarkdownToJsx>[0]["options"];

interface MarkdownToJsxProps {
  className?: string;
  content: string;
  options?: Options;
}

function preprocessMarkdown(markdown: string): string {
  // Remove frontmatter (content wrapped in ---) from the beginning of the markdown.
  let preprocessed = markdown.replace(/^\s*---\n[\s\S]*?\n---\n/, "");

  // Ensure there's an empty line before and after <details> tag if there isn't one
  // already, to ensure that it is parsed as its own component.
  // The motivation for this is that Docusaurus renders <details> properly even if
  // there aren't empty lines surrounding it.
  preprocessed = preprocessed.replace(/([^\n])\n<details>/g, "$1\n\n<details>");
  preprocessed = preprocessed.replace(/<\/details>\n([^\n])/g, "</details>\n\n$1");

  // Replace docusaurus-style admonitions with custom admonition component.
  preprocessed = preprocessed.replace(
    /:::(info|warning|note|tip|caution|danger)\s*\n([\s\S]*?)\n\s*:::\s*\n/g,
    '<admonition type="$1">$2</admonition>\n'
  );

  // Apply remark plugins to the markdown.
  // This should be ran last so that remarkGfm doesn't interfere with the above.
  preprocessed = remark().use(remarkGfm).processSync(preprocessed).toString();

  return preprocessed;
}

const Pre = ({ children, className, ...props }: { children: ReactNode; className?: string }) => {
  return (
    <pre className={classNames(className, styles.codeBlock)} {...props}>
      {React.isValidElement(children) && children.type === "code" ? (
        <CopyButton className={styles.codeCopyButton} content={children.props.children} />
      ) : null}
      {children}
    </pre>
  );
};

const Details = ({ children }: { children: ReactNode; className?: string }) => {
  const detailsChildren = React.Children.toArray(children);
  const [firstChild, ...restChildren] = detailsChildren;

  let collapsibleChildren = detailsChildren;
  let summaryText = "";

  if (React.isValidElement(firstChild) && firstChild.type === "summary") {
    summaryText = firstChild.props.children;
    collapsibleChildren = restChildren;
  }

  return (
    <Collapsible className={styles.details} buttonClassName={styles.detailsButton} label={summaryText}>
      {collapsibleChildren}
    </Collapsible>
  );
};

const Admonition = ({ children, type }: { children: ReactNode; type: string }) => {
  return <div className={classNames(styles.admonition, styles[`admonition--${type}`])}>{children}</div>;
};

export const Markdown: React.FC<MarkdownToJsxProps> = ({ className, content, options }) => {
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
          },
        }}
      >
        {preprocessMarkdown(content)}
      </MarkdownToJsx>
    </div>
  );
};
