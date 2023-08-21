import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";
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

const FIRST_NAME = {
  label: "First Name",
  name: "firstName",
  fieldType: "input",
};

const LAST_NAME = {
  label: "Last Name",
  name: "lastName",
  fieldType: "input",
};

jest.setTimeout(20000);

describe(`${Form.name}`, () => {
  it("should call onSubmit upon submission", async () => {
    const mockOnSubmit = jest.fn();
    await render(
      <Form
        schema={mockSchema}
        defaultValues={mockDefaultValues}
        onSubmit={(values) => Promise.resolve(mockOnSubmit(values))}
      >
        <FormControl name={FIRST_NAME.name} label={FIRST_NAME.label} fieldType="input" />
        <FormControl name={LAST_NAME.name} label={LAST_NAME.label} fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
    await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    await userEvent.click(submitButton);

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
        <FormControl name="firstName" label={FIRST_NAME.label} fieldType="input" />
        <FormControl name="lastName" label={LAST_NAME.label} fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
    await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    await userEvent.click(submitButton);

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
        <FormControl name={FIRST_NAME.name} label={FIRST_NAME.label} fieldType="input" />
        <FormControl name={LAST_NAME.name} label={LAST_NAME.label} fieldType="input" />
        <FormSubmissionButtons />
      </Form>
    );

    await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
    await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");
    const submitButton = screen.getAllByRole("button").filter((button) => button.getAttribute("type") === "submit")[0];
    await waitFor(() => expect(submitButton).toBeEnabled());
    await userEvent.click(submitButton);

    await waitFor(() => expect(mockOnError).toHaveBeenCalledTimes(1));
  });

  describe("reinitializeDefaultValues", () => {
    const mockStartDefaultValues = {
      firstName: "John",
      lastName: "Doe",
    };

    const mockEndDefaultValues = {
      firstName: "Jane",
      lastName: "Smith",
    };

    const CHANGE_DEFAULT_VALUES_BUTTON_TEXT = "Change default values";

    // A simple wrapper that will change the default values of the form
    const MockReinitializeForm = ({ reinitializeDefaultValues }: { reinitializeDefaultValues?: boolean }) => {
      const [defaultValues, setDefaultValues] = React.useState(mockStartDefaultValues);

      return (
        <>
          <Form
            schema={mockSchema}
            defaultValues={defaultValues}
            onSubmit={() => Promise.resolve()}
            reinitializeDefaultValues={reinitializeDefaultValues}
          >
            <FormControl name={FIRST_NAME.name} label={FIRST_NAME.label} fieldType="input" />
            <FormControl name={LAST_NAME.name} label={LAST_NAME.label} fieldType="input" />
            <FormSubmissionButtons />
          </Form>
          <button onClick={() => setDefaultValues(mockEndDefaultValues)} type="button">
            {CHANGE_DEFAULT_VALUES_BUTTON_TEXT}
          </button>
        </>
      );
    };

    it("does not reinitialize default values by default", async () => {
      await render(<MockReinitializeForm />);

      await userEvent.click(screen.getByText(CHANGE_DEFAULT_VALUES_BUTTON_TEXT));

      await waitFor(() =>
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue(mockStartDefaultValues.firstName)
      );
    });

    it("reinitializes default values when reinitializeDefaultValues is true", async () => {
      await render(<MockReinitializeForm reinitializeDefaultValues />);

      await userEvent.click(screen.getByText(CHANGE_DEFAULT_VALUES_BUTTON_TEXT));

      await waitFor(() => expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue(mockEndDefaultValues.firstName));
    });

    it("does not reinitialize default values if the form is dirty", async () => {
      const NEW_FIRST_NAME = "Susan";

      await render(<MockReinitializeForm reinitializeDefaultValues />);

      await userEvent.click(screen.getByText(CHANGE_DEFAULT_VALUES_BUTTON_TEXT));

      // By making the form dirty, reinitialization of default values should not occur
      await userEvent.clear(screen.getByLabelText(FIRST_NAME.label));
      await userEvent.type(screen.getByLabelText(FIRST_NAME.label), NEW_FIRST_NAME);

      await waitFor(() => expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue(NEW_FIRST_NAME));
    });
  });

  describe("resetValues", () => {
    interface TestFormValues {
      firstName: string;
      lastName: string;
    }

    const defaultValues = {
      firstName: "",
      lastName: "",
    };

    const TestForm: React.FC<{ onSubmit: (values: TestFormValues) => Promise<{ resetValues: TestFormValues }> }> = ({
      onSubmit,
    }) => {
      return (
        <Form<TestFormValues> schema={mockSchema} defaultValues={defaultValues} onSubmit={onSubmit}>
          <FormControl name={FIRST_NAME.name} label={FIRST_NAME.label} fieldType="input" />
          <FormControl name={LAST_NAME.name} label={LAST_NAME.label} fieldType="input" />
          <FormSubmissionButtons />
        </Form>
      );
    };

    it("should reset form with empty field values(default values) after submission", async () => {
      const onSubmit = async (values: TestFormValues) => {
        await Promise.resolve(values);
        return {
          resetValues: defaultValues,
        };
      };

      await render(<TestForm onSubmit={onSubmit} />);

      await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
      await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("John");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("Doe");
      });

      const submitButton = screen
        .getAllByRole("button")
        .filter((button) => button.getAttribute("type") === "submit")[0];
      await userEvent.click(submitButton);

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("");
      });
    });

    it("should reset form with desired field values after submission", async () => {
      const onSubmit = async (values: TestFormValues) => {
        await Promise.resolve(values);
        return {
          resetValues: { firstName: "Jane", lastName: "Smith" },
        };
      };

      await render(<TestForm onSubmit={onSubmit} />);

      await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
      await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("John");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("Doe");
      });

      const submitButton = screen
        .getAllByRole("button")
        .filter((button) => button.getAttribute("type") === "submit")[0];
      await userEvent.click(submitButton);

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("Jane");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("Smith");
      });
    });

    it("should NOT reset form with empty field values(default values) after unsuccessful submission", async () => {
      const onSubmit = async (values: TestFormValues) => {
        await Promise.reject(values);
        return {
          resetValues: defaultValues,
        };
      };

      await render(<TestForm onSubmit={onSubmit} />);

      await userEvent.type(screen.getByLabelText(FIRST_NAME.label), "John");
      await userEvent.type(screen.getByLabelText(LAST_NAME.label), "Doe");

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("John");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("Doe");
      });

      const submitButton = screen
        .getAllByRole("button")
        .filter((button) => button.getAttribute("type") === "submit")[0];
      await userEvent.click(submitButton);

      await waitFor(() => {
        expect(screen.getByLabelText(FIRST_NAME.label)).toHaveValue("John");
        expect(screen.getByLabelText(LAST_NAME.label)).toHaveValue("Doe");
      });
    });
  });
});
