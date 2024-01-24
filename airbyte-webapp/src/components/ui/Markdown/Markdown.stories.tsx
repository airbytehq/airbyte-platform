import { ComponentStory, ComponentMeta } from "@storybook/react";

import { Markdown } from "./Markdown";
import { Card } from "../Card";

export default {
  title: "UI/Markdown",
  component: Markdown,
  argTypes: {},
} as ComponentMeta<typeof Markdown>;

const Template: ComponentStory<typeof Markdown> = (args) => (
  <div>
    <Card withPadding>
      <Markdown {...args} />
    </Card>
  </div>
);

const content = `# Heading 1
## Heading 2
### Heading 3
#### Heading 4
##### Heading 5
###### Heading 6

## Basic text formatting
Ordinary text makes newline-delimited paragraphs.

    Indenting with four spaces makes a code block.

\`\`\`javascript
function codeBlock() {
  console.log("surrounding content with triple backticks does too");
}
\`\`\`

> Prefacing one or more contiguous lines with greater-than signs make block quotes.
>
> -- Abe Lincoln

[Link](https://www.airbyte.com/)

\`Pre\`

*italic*

**bold**

~strikethrough~

| Heading 1 | Heading 2 |
|:----------|:----------|
|Cell 1     | Cell 2    |
|Cell 3     | Cell 4    |

- List item 1
- List item 2
- List item 3

1. List item 1
2. List item 2
3. List item 3

* List item 1
* List item 2
* List item 3

## Custom markdown extensions
### Dropdowns
<details>

  <summary>\`details\` tags hide extra content with dropdowns</summary>

  You define the dropdown title with a \`summary\` tag, which should be the first part of
  the details tag's content. Other content is hidden until the user clicks the dropdown.

</details>

### Tabs
<Tabs>
  <TabItem value="first" label="First tab">
    Show selectable, mutually-exclusive content alternatives with tabs. The first tab is
    selected by default.
  </TabItem>
  <TabItem value="second" label="Second tab">
    Users can select a tab to render their choice of content. You've chosen the second tab!
  </TabItem>
</Tabs>

### Admonitions
:::note
  This is a note admonition
:::

:::tip
  This is a tip admonition
:::

:::info
  This is a info admonition
:::

:::caution
  This is a caution admonition
:::

:::warning
  This is a warning admonition
:::

:::danger
  This is a danger admonition
:::

### Hidden content
<HideInUI>
  Users will not see this content in the Airbyte UI, though it will be visible when
  rendered by other tools (e.g. docusaurus).
</HideInUI>

`;

export const Primary = Template.bind({});
Primary.args = {
  content,
};
