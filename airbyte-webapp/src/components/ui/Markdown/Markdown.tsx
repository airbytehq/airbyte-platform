import type { PluggableList } from "react-markdown/lib/react-markdown";

import { faCopy } from "@fortawesome/free-regular-svg-icons";
import { faCheck } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import classNames from "classnames";
import React, { useState } from "react";
import { useIntl } from "react-intl";
import ReactMarkdown from "react-markdown";
import { useCopyToClipboard, useUpdateEffect } from "react-use";
import remarkDirective from "remark-directive";
import remarkFrontmatter from "remark-frontmatter";
import remarkGfm from "remark-gfm";

import { Button } from "components/ui/Button";

import styles from "./Markdown.module.scss";
import { remarkAdmonitionsPlugin } from "./remarkAdmonitionsPlugin";

interface MarkdownProps {
  content?: string;
  className?: string;
  rehypePlugins?: PluggableList;
}

type CodeElementType = NonNullable<React.ComponentProps<typeof ReactMarkdown>["components"]>["code"];

/**
 * Custom `code` component with a copy button to copy all text in case it's a code block (and not inline code).
 */
const Code: CodeElementType = ({ children, node, inline, className, ...props }) => {
  const { formatMessage } = useIntl();
  const [, copyToClipboard] = useCopyToClipboard();
  const [hasCopied, setHasCopied] = useState(false);

  useUpdateEffect(() => {
    // Whenever the "Copied" text is shown start a timer to switch it back to the original state after 3s
    if (!hasCopied) {
      return;
    }
    const timeout = window.setTimeout(() => setHasCopied(false), 3000);
    return () => window.clearTimeout(timeout);
  }, [hasCopied]);

  const copy = (text: string) => {
    setHasCopied(true);
    copyToClipboard(text);
  };

  return (
    <code className={classNames(className, styles.codeBlock)} {...props}>
      {!inline && node.children[0].type === "text" && (
        <Button
          variant="light"
          type="button"
          icon={hasCopied ? <FontAwesomeIcon icon={faCheck} /> : undefined}
          onClick={() => node.children[0].type === "text" && copy(node.children[0].value)}
          className={styles.codeCopyButton}
          title={formatMessage({ id: "ui.markdown.copyCode" })}
        >
          {hasCopied ? formatMessage({ id: "ui.markdown.copied" }) : <FontAwesomeIcon icon={faCopy} />}
        </Button>
      )}
      {children}
    </code>
  );
};

export const Markdown: React.FC<MarkdownProps> = React.memo(({ content, className, rehypePlugins }) => {
  return (
    <ReactMarkdown
      // Open everything except fragment only links in a new tab
      linkTarget={(href) => (href.startsWith("#") ? undefined : "_blank")}
      className={classNames(styles.markdown, className)}
      skipHtml
      // This is not actually causing any issues, but requires to disable TS on this for now.
      remarkPlugins={[remarkDirective, remarkAdmonitionsPlugin, remarkFrontmatter, remarkGfm]}
      rehypePlugins={rehypePlugins}
      children={content || ""}
      components={{
        code: Code,
      }}
    />
  );
});
