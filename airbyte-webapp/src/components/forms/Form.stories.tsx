import { StoryObj } from "@storybook/react";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer, FlexItem } from "components/ui/Flex";

import { Form, FormControl, Option } from "./index";

interface MyFormValues {
  some_input: string;
  some_password: string;
  some_date: string;
  some_select: string;
}

const schema: SchemaOf<MyFormValues> = yup.object({
  some_input: yup.string().required("This is a required field."),
  some_password: yup
    .string()
    .min(5, "The password needs to be at least 5 characters long.")
    .required("This is a required field."),
  some_date: yup.string().required("This is a required field."),
  some_select: yup.string().required("This is a required field."),
});

export default {
  title: "Forms",
  component: Form,
  parameters: { actions: { argTypesRegex: "^on.*" } },
} as StoryObj<typeof Form>;

const defaultValues: MyFormValues = {
  some_input: "",
  some_password: "3mnv0dkln2%#@9fds",
  some_date: "",
  some_select: "",
};

const listBoxOptions: Array<Option<string>> = ["one", "two", "three"].map((v) => ({ label: v, value: v }));

const MyFormControl = FormControl<MyFormValues>;

export const Primary: StoryObj<typeof Form> = {
  render: (props) => (
    <Card withPadding>
      <Form {...props} schema={schema} defaultValues={defaultValues}>
        <MyFormControl
          fieldType="input"
          name="some_input"
          label="A default text input"
          description="Some default message that appears under the label"
        />
        <MyFormControl
          fieldType="input"
          type="password"
          name="some_password"
          label="Password input"
          labelTooltip={
            <>
              <p>A tooltip to give the user more context. Can also include HTML:</p>
              <ol>
                <li>One</li>
                <li>Two</li>
                <li>Three</li>
              </ol>
            </>
          }
        />
        <MyFormControl fieldType="date" name="some_date" format="date-time" label="Date input" />
        <MyFormControl fieldType="dropdown" name="some_select" label="DropDown input" options={listBoxOptions} />
        <FlexContainer justifyContent="flex-end">
          <FlexItem>
            <Button type="submit">Submit</Button>
          </FlexItem>
        </FlexContainer>
      </Form>
    </Card>
  ),
};
