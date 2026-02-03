import MarkdownToJsx, { MarkdownToJSX } from "markdown-to-jsx";
import React, { useMemo } from "react";

interface SafeMarkdownProps {
  content: string;
  onLinkClick?: (url: string, text: string) => void;
}

class MarkdownErrorBoundary extends React.Component<
  React.PropsWithChildren<{ fallback: React.ReactNode }>,
  { hasError: boolean }
> {
  constructor(props: React.PropsWithChildren<{ fallback: React.ReactNode }>) {
    super(props);
    this.state = { hasError: false };
  }
  static getDerivedStateFromError() {
    return { hasError: true };
  }
  componentDidCatch() {
    // Swallow markdown parsing/rendering errors and show fallback
  }
  render() {
    if (this.state.hasError) {
      return this.props.fallback;
    }
    return this.props.children as React.ReactElement;
  }
}

export const SafeMarkdown: React.FC<SafeMarkdownProps> = ({ content, onLinkClick }) => {
  const markdownOptions: MarkdownToJSX.Options = useMemo(
    () => ({
      disableParsingRawHTML: true,
      forceWrapper: true,
      overrides: {
        a: {
          component: ({ children, href, ...props }: React.AnchorHTMLAttributes<HTMLAnchorElement>) => (
            <a
              {...props}
              href={href}
              rel="noreferrer noopener"
              target="_blank"
              onClick={() => {
                if (onLinkClick && href) {
                  onLinkClick(href, String(children));
                }
              }}
            >
              {children}
            </a>
          ),
        },
      },
    }),
    [onLinkClick]
  );

  return (
    <MarkdownErrorBoundary fallback={<>{content}</>}>
      <MarkdownToJsx options={markdownOptions}>{content}</MarkdownToJsx>
    </MarkdownErrorBoundary>
  );
};
