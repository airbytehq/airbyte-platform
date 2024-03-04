import { render as renderUnwrapped, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { TestWrapper } from "test-utils/testutils";

import { Markdown } from "./Markdown";
import admonitionStyles from "./overrides/Admonition.module.scss";
import detailsStyles from "./overrides/Details.module.scss";
import preStyles from "./overrides/Pre.module.scss";

const render = (componentSubtree: React.ReactElement) => {
  const rendered = renderUnwrapped(<TestWrapper>{componentSubtree}</TestWrapper>);

  return {
    queryByStyle(className: string) {
      return rendered.container.querySelector(`.${className}`);
    },
    ...rendered,
  };
};

describe("Markdown rendering", () => {
  it("basic markdown content", () => {
    const md = `# Heading

Paragraph text.

- a list
- with *wild* and _crazy_ formatting
- but no sub-lists
`;

    const { container } = render(<Markdown content={md} />);

    expect(container.querySelector("h1")).toBeInTheDocument();
    expect(container.querySelector("p")).toBeInTheDocument();
    expect(container.querySelector("li")).toBeInTheDocument();
    expect(container.querySelectorAll("ul").length).toBe(1);
  });

  it("Details", async () => {
    const md = `
<details>

<summary>titular summary</summary>

dropdown contents

</details>
`;

    const user = userEvent.setup();
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(detailsStyles.details)).toBeInTheDocument();
    expect(screen.queryByText("titular summary")).toBeVisible();
    expect(screen.queryByText("dropdown contents")).not.toBeVisible();

    await user.click(queryByStyle(detailsStyles.detailsButton)!);
    expect(await screen.findByText("dropdown contents")).toBeVisible();
  });

  it("Docusaurus-style admonitions", () => {
    const md = `
# Admonish your friends privately, but praise them openly

:::info
This is an information admonition
:::
`;

    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(admonitionStyles.admonition)).toBeInTheDocument();
  });

  it("Docusaurus-style tabs", async () => {
    const md = `
<Tabs>
<TabItem value="first" label="first tab">
thing one
</TabItem>
<TabItem value="second" label="second tab">
thing two
</TabItem>
</Tabs>
`;

    const user = userEvent.setup();
    render(<Markdown content={md} />);

    // Annoyingly, we cannot use an assertion like
    // `expect(screen.queryByText(unselectedTabContents)).not.toBeVisible()`
    // because the query returns `null` instead of an element, causing `.toBeVisible` to
    // throw an error; and so we have to change our test code because of the
    // implementation detail that unselected tabs are completely unrendered instead of
    // just being hidden with `display: none;` or such.
    expect(screen.queryByText("thing one")).toBeInTheDocument();
    expect(screen.queryByText("thing two")).not.toBeInTheDocument();

    await user.click(screen.queryByText("second tab")!);

    expect(screen.queryByText("thing two")).toBeInTheDocument();
    expect(screen.queryByText("thing one")).not.toBeInTheDocument();
  });

  it("HideInUI", () => {
    const md = `
visible content before

<HideInUI>
this should not be rendered
</HideInUI>

visible content after
`;

    render(<Markdown content={md} />);

    expect(screen.queryByText("visible content before")).toBeInTheDocument();
    expect(screen.queryByText("visible content after")).toBeInTheDocument();
    expect(screen.queryByText("this should not be rendered")).not.toBeInTheDocument();
  });

  it("GFM tables", () => {
    const md = `
| Header 1 | Header 2 |
|----------|----------|
| Row 1    | Data 1   |
| Row 2    | Data 2   |
`;

    const { container } = render(<Markdown content={md} />);

    // There should be a table with three rows (header + two rows)
    expect(container.querySelector("table")).toBeInTheDocument();
    expect(container.querySelectorAll("tr").length).toBe(3); // Including the header row
  });

  it("fancy code blocks", () => {
    const md = "```\nthis is a code block\n```";
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(preStyles.codeBlock)).toBeInTheDocument();
    expect(queryByStyle(preStyles.codeCopyButton)).toBeInTheDocument();
  });
});

describe("preprocessing markdown strings", () => {
  it("removes frontmatter", () => {
    const md = `---
frontmatter: hello
please: delete me
---

Other markdown content
`;

    render(<Markdown content={md} />);

    expect(screen.queryByText("frontmatter")).toBeFalsy();
    expect(screen.queryByText("Other markdown content")).toBeDefined();
  });
});
