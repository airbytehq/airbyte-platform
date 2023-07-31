import { action } from "@storybook/addon-actions";
import { StoryObj } from "@storybook/react";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { Card } from "components/ui/Card";

import { FormSubmissionButtons } from "./FormSubmissionButtons";

import { Form, FormControl, Option } from "./index";

interface MyFormValues {
  some_input: string;
  some_textarea: string;
  some_password: string;
  some_date: string;
  some_select: string;
}

const schema: SchemaOf<MyFormValues> = yup.object({
  some_input: yup.string().required("This is a required field."),
  some_textarea: yup.string().required("This is a required field."),
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
  argTypes: {
    disabled: { control: "boolean" },
  },
} as StoryObj<typeof Form>;

const defaultValues: MyFormValues = {
  some_input: "",
  some_textarea: "",
  some_password: "3mnv0dkln2%#@9fds",
  some_date: "",
  some_select: "",
};

const listBoxOptions: Array<Option<string>> = ["one", "two", "three"].map((v) => ({ label: v, value: v }));

const MyFormControl = FormControl<MyFormValues>;

export const Primary: StoryObj<typeof Form> = {
  render: (props) => (
    <div style={{ maxWidth: "1200px" }}>
      <Card withPadding>
        <Form
          {...props}
          schema={schema}
          defaultValues={defaultValues}
          onSuccess={action("onSuccess")}
          onSubmit={() => new Promise((resolve) => window.setTimeout(resolve, 1000))}
        >
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
          <MyFormControl fieldType="textarea" name="some_textarea" label="Text area" rows={5} />
          <MyFormControl fieldType="date" name="some_date" format="date-time" label="Date input" />
          <MyFormControl fieldType="dropdown" name="some_select" label="DropDown input" options={listBoxOptions} />
          <FormSubmissionButtons />
          <button>Click to submit, even if there are errors</button>
        </Form>
      </Card>
    </div>
  ),
};

export const InlineFormControls: StoryObj<typeof Form> = {
  render: (props) => (
    <div style={{ maxWidth: "1200px" }}>
      <Card withPadding>
        <Form
          {...props}
          schema={schema}
          defaultValues={defaultValues}
          onSuccess={action("onSuccess")}
          onSubmit={() => new Promise((resolve) => window.setTimeout(resolve, 1000))}
        >
          <MyFormControl
            inline
            fieldType="input"
            name="some_input"
            label="A default text input"
            description="Some default message that appears under the label"
          />
          <MyFormControl
            inline
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
          <MyFormControl inline fieldType="date" name="some_date" format="date-time" label="Date input" />
          <MyFormControl
            inline
            fieldType="dropdown"
            name="some_select"
            label="DropDown input"
            options={listBoxOptions}
          />
          <FormSubmissionButtons />
          <button>Click to submit, even if there are errors</button>
        </Form>
      </Card>
    </div>
  ),
};
