import MarkdownToJsx, { MarkdownToJSX } from "markdown-to-jsx";
import React from "react";

interface SafeMarkdownProps {
  content: string;
}

const markdownOptions: MarkdownToJSX.Options = {
  disableParsingRawHTML: true,
  forceWrapper: true,
  overrides: {
    a: {
      props: {
        rel: "noreferrer noopener",
        target: "_blank",
      },
    },
  },
};

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

export const SafeMarkdown: React.FC<SafeMarkdownProps> = ({ content }) => {
  return (
    <MarkdownErrorBoundary fallback={<>{content}</>}>
      <MarkdownToJsx options={markdownOptions}>{content}</MarkdownToJsx>
    </MarkdownErrorBoundary>
  );
};
