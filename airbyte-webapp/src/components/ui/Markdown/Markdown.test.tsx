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

  it("renders links with inline-code text in GFM table cells", () => {
    const md = `
| Stream |
| :--- |
| [\`activity_feed\`](https://example.com/a) |

[Activity feed](https://example.com/activity-feed)
`;

    const { container } = render(<Markdown content={md} />);

    const inlineCodeLink = screen.getByRole("link", { name: "activity_feed" });
    expect(inlineCodeLink).toHaveAttribute("href", "https://example.com/a");
    expect(inlineCodeLink.querySelector("code")).toHaveTextContent("activity_feed");
    expect(container.querySelector("table")).toBeInTheDocument();
    expect(inlineCodeLink.closest("td")).not.toBeNull();

    expect(screen.getByRole("link", { name: "Activity feed" })).toHaveAttribute(
      "href",
      "https://example.com/activity-feed"
    );
  });

  it("fancy code blocks", () => {
    const md = "```\nthis is a code block\n```";
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(preStyles.codeBlock)).toBeInTheDocument();
    expect(queryByStyle(preStyles.codeCopyButton)).toBeInTheDocument();
  });

  it("renders a normal markdown list without zero-width space characters", () => {
    const md = `
- First item
- Second item **+ add**
`;
    render(<Markdown content={md} />);
    const listItems = document.querySelectorAll("li");
    expect(listItems.length).toBe(2);
    expect(listItems[0].textContent).toBe("First item");
    expect(listItems[1].textContent).toContain("Second item");

    // Check that <strong> exists and contains '+ add'
    const strong = listItems[1].querySelector("strong");
    expect(strong).toBeInTheDocument();
    expect(strong?.textContent).toContain("+ add");
  });
});

const ADMONITION_TYPES = ["info", "warning", "note", "tip", "caution", "danger"] as const;

describe("admonitions with a title", () => {
  const md = `
:::info Direct Load
The body of the admonition.
:::
`;

  it("should render as a box", () => {
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(admonitionStyles.admonition)).toBeInTheDocument();
  });

  it("should render as a box of its type", () => {
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(admonitionStyles["admonition--info"])).toBeInTheDocument();
  });

  it("should show the title as the heading of the box", () => {
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(admonitionStyles.admonition__title)).toHaveTextContent("Direct Load");
  });

  it("should show the body in the box", () => {
    const { queryByStyle } = render(<Markdown content={md} />);

    expect(queryByStyle(admonitionStyles.admonition)).toHaveTextContent("The body of the admonition.");
  });

  it("should not leave the raw syntax visible", () => {
    const { container } = render(<Markdown content={md} />);

    expect(container.textContent).not.toContain(":::");
  });

  it.each(ADMONITION_TYPES)("should render a titled %s admonition as a box of that type", (type) => {
    const { queryByStyle } = render(<Markdown content={`\n:::${type} A title\nThe body.\n:::\n`} />);

    expect(queryByStyle(admonitionStyles[`admonition--${type}`])).toBeInTheDocument();
  });

  it("should render inline code in the title", () => {
    const { queryByStyle } = render(<Markdown content={"\n:::note Behavior change in `0.2.23`\nThe body.\n:::\n"} />);

    expect(queryByStyle(admonitionStyles.admonition__title)?.querySelector("code")).toHaveTextContent("0.2.23");
  });

  it("should render a title containing double quotes in full", () => {
    const { queryByStyle } = render(<Markdown content={'\n:::note Say "hello"\nThe body.\n:::\n'} />);

    expect(queryByStyle(admonitionStyles.admonition__title)).toHaveTextContent(/^Say .hello.$/);
  });

  it("should not show a title for an admonition without one", () => {
    const { queryByStyle } = render(<Markdown content={"\n:::info\nThe body.\n:::\n"} />);

    expect(queryByStyle(admonitionStyles.admonition__title)).not.toBeInTheDocument();
  });
});

describe("admonitions inside a list item", () => {
  const listWith = (opening: string) => `
1. First step.

   ${opening}
   Text of the admonition.
   :::
2. Second step.
`;

  describe("without a title", () => {
    it("should render as a box inside the list item", () => {
      const { container } = render(<Markdown content={listWith(":::note")} />);

      expect(container.querySelector(`li .${admonitionStyles.admonition}`)).toBeInTheDocument();
    });

    it("should keep the list in one piece", () => {
      const { container } = render(<Markdown content={listWith(":::note")} />);

      expect(container.querySelectorAll("ol")).toHaveLength(1);
    });

    it("should not show a literal admonition tag", () => {
      const { container } = render(<Markdown content={listWith(":::note")} />);

      expect(container.textContent).not.toContain("</admonition>");
    });

    it("should show the text in the box", () => {
      const { queryByStyle } = render(<Markdown content={listWith(":::note")} />);

      expect(queryByStyle(admonitionStyles.admonition)).toHaveTextContent("Text of the admonition.");
    });
  });

  describe("with a title", () => {
    it("should render as a box inside the list item", () => {
      const { container } = render(<Markdown content={listWith(":::note HubSpot Authentication issues")} />);

      expect(container.querySelector(`li .${admonitionStyles.admonition}`)).toBeInTheDocument();
    });

    it("should show the title as the heading of the box", () => {
      const { queryByStyle } = render(<Markdown content={listWith(":::note HubSpot Authentication issues")} />);

      expect(queryByStyle(admonitionStyles.admonition__title)).toHaveTextContent("HubSpot Authentication issues");
    });

    it("should keep the list in one piece", () => {
      const { container } = render(<Markdown content={listWith(":::note HubSpot Authentication issues")} />);

      expect(container.querySelectorAll("ol")).toHaveLength(1);
    });

    it("should not leave the raw syntax visible", () => {
      const { container } = render(<Markdown content={listWith(":::note HubSpot Authentication issues")} />);

      expect(container.textContent).not.toContain(":::");
    });
  });

  it("should render a callout indented under a bullet, as the HubSpot docs do", () => {
    const md = `
- (Recommended) Authenticate with OAuth.

     :::note HubSpot Authentication issues
     You may encounter an error during the authentication process.
     :::
- (Not Recommended) Use a Private App.
`;
    const { container } = render(<Markdown content={md} />);

    expect(container.querySelector(`li .${admonitionStyles.admonition}`)).toBeInTheDocument();
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
