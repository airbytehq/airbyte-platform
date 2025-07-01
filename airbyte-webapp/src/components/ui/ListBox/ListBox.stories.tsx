import { Meta, StoryFn } from "@storybook/react";
import { useState } from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { ListBox, ListBoxProps, MIN_OPTIONS_FOR_VIRTUALIZATION } from "./ListBox";

export default {
  title: "Ui/ListBox",
  component: ListBox,
  argTypes: {
    placement: {
      options: [
        "top",
        "top-start",
        "top-end",
        "right",
        "right-start",
        "right-end",
        "bottom",
        "bottom-start",
        "bottom-end",
        "left",
        "left-start",
        "left-end",
      ],
      control: { type: "radio" },
    },
  },
} as Meta<typeof ListBox>;

const Template: StoryFn<typeof ListBox> = <T,>(args: Omit<ListBoxProps<T>, "onSelect">) => {
  const [selectedValue, setSelectedValue] = useState(args.selectedValue);
  return <ListBox {...args} selectedValue={selectedValue} onSelect={setSelectedValue} />;
};

const options = [
  {
    label: "one",
    value: 1,
    icon: "pencil",
  },
  {
    label: "two",
    value: 2,
  },
  {
    label: "three",
    value: 3,
  },
];

export const Primary = Template.bind({});
Primary.args = {
  options,
  selectedValue: 1,
};

export const Placement = Template.bind({});
Placement.args = {
  options,
  adaptiveWidth: false,
};
Placement.decorators = [
  (Story: StoryFn) => (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      style={{ width: 500, height: 500, border: "1px solid #494961" }}
    >
      <Story />
    </FlexContainer>
  ),
];

export const WithCustomOptionIcon = Template.bind({});
WithCustomOptionIcon.args = {
  options: [
    ...options,
    {
      label: "four",
      value: 4,
      icon: <Icon type="check" color="success" size="lg" />,
    },
  ],
  adaptiveWidth: false,
};

export const TextWrap = Template.bind({});
TextWrap.args = {
  options: [...options, { label: "This is a very long label that should wrap", value: 4 }],
  adaptiveWidth: false,
};
TextWrap.decorators = [
  (Story: StoryFn) => (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      style={{ width: 500, height: 500, border: "1px solid #494961" }}
    >
      <Story />
    </FlexContainer>
  ),
];

export const ValueAsObject = Template.bind({});
ValueAsObject.args = {
  options: [
    {
      label: "Basic",
      value: { scheduleType: "basic" },
    },
    {
      label: "Manual",
      value: { scheduleType: "manual" },
      disabled: true,
    },
    {
      label: "Cron",
      value: { scheduleType: "cron" },
    },
  ],
  selectedValue: { scheduleType: "basic" },
};

const countries = [
  "Afghanistan",
  "Albania",
  "Algeria",
  "Andorra",
  "Angola",
  "Antigua and Barbuda",
  "Argentina",
  "Armenia",
  "Australia",
  "Austria",
  "Azerbaijan",
  "Bahamas",
  "Bahrain",
  "Bangladesh",
  "Barbados",
  "Belarus",
  "Belgium",
  "Belize",
  "Benin",
  "Bhutan",
  "Bolivia",
  "Bosnia and Herzegovina",
  "Botswana",
  "Brazil",
  "Brunei",
  "Bulgaria",
  "Burkina Faso",
  "Burundi",
  "Cabo Verde",
  "Cambodia",
  "Cameroon",
  "Canada",
  "Central African Republic",
  "Chad",
  "Chile",
  "China",
  "Colombia",
  "Comoros",
  "Congo (Congo-Brazzaville)",
  "Costa Rica",
  "Croatia",
  "Cuba",
  "Cyprus",
  "Czechia (Czech Republic)",
  "Democratic Republic of the Congo",
  "Denmark",
  "Djibouti",
  "Dominica",
  "Dominican Republic",
  "Ecuador",
  "Egypt",
  "El Salvador",
  "Equatorial Guinea",
  "Eritrea",
  "Estonia",
  "Eswatini (fmr. 'Swaziland')",
  "Ethiopia",
  "Fiji",
  "Finland",
  "France",
  "Gabon",
  "Gambia",
  "Georgia",
  "Germany",
  "Ghana",
  "Greece",
  "Grenada",
  "Guatemala",
  "Guinea",
  "Guinea-Bissau",
  "Guyana",
  "Haiti",
  "Honduras",
  "Hungary",
  "Iceland",
  "India",
  "Indonesia",
  "Iran",
  "Iraq",
  "Ireland",
  "Israel",
  "Italy",
  "Jamaica",
  "Japan",
  "Jordan",
  "Kazakhstan",
  "Kenya",
  "Kiribati",
  "Kuwait",
  "Kyrgyzstan",
  "Laos",
  "Latvia",
  "Lebanon",
  "Lesotho",
  "Liberia",
  "Libya",
  "Liechtenstein",
  "Lithuania",
  "Luxembourg",
  "Madagascar",
  "Malawi",
  "Malaysia",
  "Maldives",
  "Mali",
  "Malta",
  "Marshall Islands",
  "Mauritania",
  "Mauritius",
  "Mexico",
  "Micronesia",
  "Moldova",
  "Monaco",
  "Mongolia",
  "Montenegro",
  "Morocco",
  "Mozambique",
  "Myanmar (formerly Burma)",
  "Namibia",
  "Nauru",
  "Nepal",
  "Netherlands",
  "New Zealand",
  "Nicaragua",
  "Niger",
  "Nigeria",
  "North Korea",
  "North Macedonia",
  "Norway",
  "Oman",
  "Pakistan",
  "Palau",
  "Palestine State",
  "Panama",
  "Papua New Guinea",
  "Paraguay",
  "Peru",
  "Philippines",
  "Poland",
  "Portugal",
  "Qatar",
  "Romania",
  "Russia",
  "Rwanda",
  "Saint Kitts and Nevis",
  "Saint Lucia",
  "Saint Vincent and the Grenadines",
  "Samoa",
  "San Marino",
  "Sao Tome and Principe",
  "Saudi Arabia",
  "Senegal",
  "Serbia",
  "Seychelles",
  "Sierra Leone",
  "Singapore",
  "Slovakia",
  "Slovenia",
  "Solomon Islands",
  "Somalia",
  "South Africa",
  "South Korea",
  "South Sudan",
  "Spain",
  "Sri Lanka",
  "Sudan",
  "Suriname",
  "Sweden",
  "Switzerland",
  "Syria",
  "Tajikistan",
  "Tanzania",
  "Thailand",
  "Timor-Leste",
  "Togo",
  "Tonga",
  "Trinidad and Tobago",
  "Tunisia",
  "Turkey",
  "Turkmenistan",
  "Tuvalu",
  "Uganda",
  "Ukraine",
  "United Arab Emirates",
  "United Kingdom",
  "United States of America",
  "Uruguay",
  "Uzbekistan",
  "Vanuatu",
  "Vatican City",
  "Venezuela",
  "Vietnam",
  "Yemen",
  "Zambia",
  "Zimbabwe",
];
const countryObjects = countries.map((country) => ({
  label: country,
  value: country,
}));

export const Virtualized = Template.bind({});
Virtualized.args = {
  options: countryObjects,
  adaptiveWidth: false,
};
Virtualized.decorators = [
  (Story: StoryFn) => (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      style={{ width: 500, height: 500, border: "1px solid #494961" }}
    >
      <Story />
    </FlexContainer>
  ),
];

export const NotVirtualized = Template.bind({});
NotVirtualized.args = {
  options: countryObjects.slice(0, MIN_OPTIONS_FOR_VIRTUALIZATION - 1),
  adaptiveWidth: false,
};
NotVirtualized.decorators = [
  (Story: StoryFn) => (
    <FlexContainer
      alignItems="center"
      justifyContent="center"
      style={{ width: 500, height: 500, border: "1px solid #494961" }}
    >
      <Story />
    </FlexContainer>
  ),
];
