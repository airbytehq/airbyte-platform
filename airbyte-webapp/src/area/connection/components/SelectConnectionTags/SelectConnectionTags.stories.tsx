import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";
import { v4 as uuidv4 } from "uuid";

import { Tag } from "core/api/types/AirbyteClient";

import { SelectConnectionTags } from "./SelectConnectionTags";

export default {
  title: "Connection/SelectConnectionTags",
  component: SelectConnectionTags,
} as Meta;

const Template: StoryFn<typeof SelectConnectionTags> = (args) => {
  const [selectedTags, setSelectedTags] = useState<Tag[]>(args.selectedTags);

  const selectTag = (selectedTag: Tag) => {
    setSelectedTags((prevTags) => [...prevTags, selectedTag]);
  };

  const deselectTag = (deselectedTag: Tag) => {
    setSelectedTags((prevTags) => prevTags.filter((tag) => tag.tagId !== deselectedTag.tagId));
  };

  const createTag = async (name: string, color: string) => {
    const newTag = {
      name,
      tagId: uuidv4(),
      workspaceId: "workspace",
      color,
    };

    setSelectedTags((prevTags) => [newTag, ...prevTags]);
  };

  return (
    <SelectConnectionTags
      availableTags={args.availableTags}
      selectedTags={selectedTags}
      selectTag={selectTag}
      deselectTag={deselectTag}
      createTag={createTag}
      disabled={args.disabled}
    />
  );
};

export const Edit = Template.bind({});
Edit.args = {
  selectedTags: [
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
  ],
  availableTags: [
    { tagId: "1", name: "Sales", color: "#FBECB1", workspaceId: "workspace" },
    { tagId: "2", name: "Ops", color: "#FEC9BE", workspaceId: "workspace" },
    { tagId: "3", name: "HR", color: "#75DCFF", workspaceId: "workspace" },
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
    { tagId: "6", name: "IT", color: "#CAC7FF", workspaceId: "workspace" },
    { tagId: "7", name: "Legal", color: "#FFE5E9", workspaceId: "workspace" },
  ],
  createTag: async (name: string) => console.log(`Create tag: ${name}`),
  selectTag: (tag: Tag) => console.log(`Select tag: ${tag.tagId}`),
  deselectTag: (tag: Tag) => console.log(`Deselect tag: ${tag.tagId}`),
};
export const EditDisabled = Template.bind({});
EditDisabled.args = {
  selectedTags: [
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
  ],
  availableTags: [
    { tagId: "1", name: "Sales", color: "#FBECB1", workspaceId: "workspace" },
    { tagId: "2", name: "Ops", color: "#FEC9BE", workspaceId: "workspace" },
    { tagId: "3", name: "HR", color: "#75DCFF", workspaceId: "workspace" },
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
    { tagId: "6", name: "IT", color: "#CAC7FF", workspaceId: "workspace" },
    { tagId: "7", name: "Legal", color: "#FFE5E9", workspaceId: "workspace" },
  ],
  createTag: async (name: string) => console.log(`Create tag: ${name}`),
  selectTag: (tag: Tag) => console.log(`Select tag: ${tag.tagId}`),
  deselectTag: (tag: Tag) => console.log(`Deselect tag: ${tag.tagId}`),
  disabled: true,
};

export const EditLimitReached = Template.bind({});
EditLimitReached.args = {
  selectedTags: [
    { tagId: "1", name: "Sales", color: "#FBECB1", workspaceId: "workspace" },
    { tagId: "2", name: "Ops", color: "#FEC9BE", workspaceId: "workspace" },
    { tagId: "3", name: "HR", color: "#75DCFF", workspaceId: "workspace" },
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
    { tagId: "6", name: "IT", color: "#CAC7FF", workspaceId: "workspace" },
    { tagId: "7", name: "Legal", color: "#FFE5E9", workspaceId: "workspace" },
    { tagId: "8", name: "Product", color: "#D4DFFC", workspaceId: "workspace" },
    { tagId: "9", name: "Support", color: "#BDFFC3", workspaceId: "workspace" },
    { tagId: "10", name: "Admin", color: "#DFD5CE", workspaceId: "workspace" },
  ],
  availableTags: [
    { tagId: "1", name: "Sales", color: "#FBECB1", workspaceId: "workspace" },
    { tagId: "2", name: "Ops", color: "#FEC9BE", workspaceId: "workspace" },
    { tagId: "3", name: "HR", color: "#75DCFF", workspaceId: "workspace" },
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
    { tagId: "6", name: "IT", color: "#CAC7FF", workspaceId: "workspace" },
    { tagId: "7", name: "Legal", color: "#FFE5E9", workspaceId: "workspace" },
    { tagId: "8", name: "Product", color: "#D4DFFC", workspaceId: "workspace" },
    { tagId: "9", name: "Support", color: "#BDFFC3", workspaceId: "workspace" },
    { tagId: "10", name: "Admin", color: "#DFD5CE", workspaceId: "workspace" },
    { tagId: "11", name: "Legacy", color: "#75DCFF", workspaceId: "workspace" },
  ],
  createTag: async (name: string) => console.log(`Create tag: ${name}`),
  selectTag: (tag: Tag) => console.log(`Select tag: ${tag.tagId}`),
  deselectTag: (tag: Tag) => console.log(`Deselect tag: ${tag.tagId}`),
  disabled: false,
};

export const DefaultDisabled = Template.bind({});
DefaultDisabled.args = {
  selectedTags: [
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
  ],
  availableTags: [
    { tagId: "1", name: "Sales", color: "#FBECB1", workspaceId: "workspace" },
    { tagId: "2", name: "Ops", color: "#FEC9BE", workspaceId: "workspace" },
    { tagId: "3", name: "HR", color: "#75DCFF", workspaceId: "workspace" },
    { tagId: "4", name: "Finance", color: "#FFD0B2", workspaceId: "workspace" },
    { tagId: "5", name: "Marketing", color: "#DDF6F8", workspaceId: "workspace" },
    { tagId: "6", name: "IT", color: "#CAC7FF", workspaceId: "workspace" },
    { tagId: "7", name: "Legal", color: "#FFE5E9", workspaceId: "workspace" },
  ],
  createTag: async (name: string) => console.log(`Create tag: ${name}`),
  selectTag: (tag: Tag) => console.log(`Select tag: ${tag.tagId}`),
  deselectTag: (tag: Tag) => console.log(`Deselect tag: ${tag.tagId}`),
  disabled: true,
};

export const EmptyTags = Template.bind({});
EmptyTags.args = {
  selectedTags: [],
  availableTags: [],
  createTag: async (name: string) => console.log(`Create tag: ${name}`),
  selectTag: (tag: Tag) => console.log(`Select tag: ${tag.tagId}`),
  deselectTag: (tag: Tag) => console.log(`Deselect tag: ${tag.tagId}`),
};
