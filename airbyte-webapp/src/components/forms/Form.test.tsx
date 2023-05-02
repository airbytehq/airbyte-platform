import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import * as yup from "yup";
import { SchemaOf } from "yup";

import { render } from "test-utils/testutils";

import { Form } from "./Form";
import { FormControl } from "./FormControl";
import { FormSubmissionButtons } from "./FormSubmissionButtons";

interface MockFormValues {
  firstName: string;
  lastName: string;
}

const mockSchema: SchemaOf<MockFormValues> = yup.object({
  firstName: yup.string().required("firstName is a required field."),
  lastName: yup.string().required("lastName is a required field."),
});

const mockDefaultValues: MockFormValues = {
  firstName: "",
  lastName: "",
};

describe(`${Form.name}`, () => {
  it("should call onSubmit upon submission", async () => {
    const mockOnSubmit = jest.fn();
    await render(
      <Form
        schema={mockSchema}
        defaultValues={mockDefaultValues}
        onSubmit={(values) => Promise.resolve(mockOnSubmit(values))}
      >
        <FormControl name="firstName" label="First Name" fieldType="input" />
        <FormControl name="lastName" label="Last Name" fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    userEvent.type(screen.getByLabelText("First Name"), "John");
    userEvent.type(screen.getByLabelText("Last Name"), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    userEvent.click(submitButton);

    await waitFor(() => expect(mockOnSubmit).toHaveBeenCalledWith({ firstName: "John", lastName: "Doe" }));
  });

  it("should call onSuccess upon success", async () => {
    const mockOnSuccess = jest.fn();
    await render(
      <Form
        schema={mockSchema}
        defaultValues={mockDefaultValues}
        onSubmit={() => Promise.resolve()}
        onSuccess={mockOnSuccess}
      >
        <FormControl name="firstName" label="First Name" fieldType="input" />
        <FormControl name="lastName" label="Last Name" fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    userEvent.type(screen.getByLabelText("First Name"), "John");
    userEvent.type(screen.getByLabelText("Last Name"), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    userEvent.click(submitButton);

    await waitFor(() => expect(mockOnSuccess).toHaveBeenCalledTimes(1));
  });

  it("should call onError upon error", async () => {
    const mockOnError = jest.fn();

    await render(
      <Form
        schema={mockSchema}
        defaultValues={mockDefaultValues}
        onSubmit={() => Promise.reject()}
        onError={mockOnError}
      >
        <FormControl name="firstName" label="First Name" fieldType="input" />
        <FormControl name="lastName" label="Last Name" fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    userEvent.type(screen.getByLabelText("First Name"), "John");
    userEvent.type(screen.getByLabelText("Last Name"), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    userEvent.click(submitButton);

    await waitFor(() => expect(mockOnError).toHaveBeenCalledTimes(1));
  });
});
