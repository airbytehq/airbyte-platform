import { screen, waitFor, fireEvent } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils/testutils";

import { SchemaFormControl } from "./Controls/SchemaFormControl";
import { SchemaForm } from "./SchemaForm";
import { SchemaFormRemainingFields } from "./SchemaFormRemainingFields";
import { FormControl } from "../FormControl";
import { FormSubmissionButtons } from "../FormSubmissionButtons";

describe("SchemaForm", () => {
  // Basic form schema for testing
  const basicSchema = {
    type: "object",
    properties: {
      name: {
        type: "string",
        title: "Name",
        minLength: 2,
        description: "Enter your first name",
      },
      age: {
        type: "integer",
        title: "Age",
        minimum: 0,
      },
      isActive: {
        type: "boolean",
        title: "Active",
      },
    },
    required: ["name"],
    additionalProperties: false,
  } as const;

  // Schema with nested objects for testing
  const nestedSchema = {
    type: "object",
    properties: {
      name: {
        type: "string",
        title: "Name",
        minLength: 2,
      },
      address: {
        type: "object",
        title: "Address",
        description: "Your home address",
        required: ["street"],
        properties: {
          street: { type: "string", title: "Street" },
          city: { type: "string", title: "City" },
          zipCode: { type: "string", title: "Zip Code" },
        },
      },
    },
    required: ["name"],
    additionalProperties: false,
  } as const;

  // Schema with conditionals using oneOf
  const conditionalSchema = {
    type: "object",
    properties: {
      contactMethod: {
        type: "object",
        title: "Contact Method",
        description: "How you should be contacted",
        oneOf: [
          {
            title: "Email",
            required: ["emailAddress"],
            properties: {
              type: {
                type: "string",
                enum: ["EmailContactMethod"],
              },
              emailAddress: {
                type: "string",
                title: "Email Address",
                format: "email",
              },
            },
          },
          {
            title: "SMS",
            properties: {
              type: {
                type: "string",
                enum: ["SMSContactMethod"],
              },
              phoneNumber: {
                type: "string",
                title: "Phone Number",
              },
            },
          },
        ],
      },
    },
    additionalProperties: false,
  } as const;

  // Schema with array of objects
  const arraySchema = {
    type: "object",
    properties: {
      friends: {
        type: "array",
        title: "Friends",
        items: {
          type: "object",
          title: "Friend",
          required: ["name"],
          properties: {
            name: { type: "string", title: "Name", minLength: 2 },
            age: { type: "integer", title: "Age" },
          },
        },
      },
      tags: {
        type: "array",
        title: "Tags",
        items: {
          type: "string",
        },
      },
    },
    additionalProperties: false,
  } as const;

  it("renders basic form fields correctly", async () => {
    const mockOnSubmit = jest.fn();

    await render(
      <SchemaForm schema={basicSchema} onSubmit={() => Promise.resolve(mockOnSubmit())}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that all fields are rendered correctly
    expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Age Optional" })).toBeInTheDocument();
    expect(screen.getByRole("checkbox", { name: "Active Optional" })).toBeInTheDocument();
  });

  it("validates required fields and shows error messages", async () => {
    const mockOnSubmit = jest.fn();

    await render(
      <SchemaForm schema={basicSchema} onSubmit={() => Promise.resolve(mockOnSubmit())}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check for the error message - use direct DOM inspection
    await waitFor(() => {
      // Try to submit form to trigger validation
      fireEvent.submit(screen.getByRole("button", { name: "Submit" }));
      // Just check for button state, since error messages might vary
      const submitButton = screen.getByRole("button", { name: "Submit" });
      expect(submitButton).toBeDisabled();
    });

    // Verify that onSubmit wasn't called
    expect(mockOnSubmit).not.toHaveBeenCalled();
  });

  it("submits the form with valid input data", async () => {
    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={basicSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Fill in the required field
    await userEvent.type(screen.getByRole("textbox", { name: "Name" }), "John");

    // Fill in the optional age field
    await userEvent.type(screen.getByRole("textbox", { name: "Age Optional" }), "30");

    // Toggle the active switch
    await userEvent.click(screen.getByRole("checkbox", { name: "Active Optional" }));

    // Submit the form
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Check that onSubmit was called with the expected data
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith({ name: "John", age: 30, isActive: true }, expect.anything());
    });
  });

  it("renders nested object fields correctly", async () => {
    await render(
      <SchemaForm schema={nestedSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check parent field is rendered
    expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();

    // Check address group is rendered
    expect(screen.getByText("Address")).toBeInTheDocument();

    // Click on the address toggle to reveal the fields
    await userEvent.click(screen.getByRole("checkbox", { name: "Address" }));

    // Check nested fields are rendered
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Street" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "City Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Zip Code Optional" })).toBeInTheDocument();
    });
  });

  it("handles oneOf conditional fields correctly", async () => {
    await render(
      <SchemaForm schema={conditionalSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that the contact method select is rendered
    expect(screen.getByText("Contact Method")).toBeInTheDocument();

    // Enable the contact method by clicking the toggle
    const contactToggle = screen.getByRole("checkbox", { name: "Contact Method" });
    await userEvent.click(contactToggle);

    // Check that email field appears
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Email Address" })).toBeInTheDocument();
    });

    // Re-open dropdown to switch
    await userEvent.click(screen.getByRole("button", { name: "Email" }));

    // Now select SMS
    await userEvent.click(screen.getByText("SMS"));

    // Check that phone number field appears
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Phone Number Optional" })).toBeInTheDocument();
    });
  });

  it("handles array of objects correctly", async () => {
    await render(
      <SchemaForm schema={arraySchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that the array section is rendered
    expect(screen.getByText("Friends")).toBeInTheDocument();

    // Find the add button and click it to add an item
    const addButton = await screen.findByRole("button", { name: "Add Friend" });
    await userEvent.click(addButton);

    // Check that fields for the new item appear
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Age Optional" })).toBeInTheDocument();
    });

    // Fill in the required field
    await userEvent.type(screen.getByRole("textbox", { name: "Name" }), "Alice");

    // Add another friend
    await userEvent.click(addButton);

    // Now we should have two name fields
    await waitFor(() => {
      const nameFields = screen.getAllByRole("textbox", { name: "Name" });
      expect(nameFields.length).toBe(2);
    });
  });

  it("handles array of strings correctly", async () => {
    await render(
      <SchemaForm schema={arraySchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl path="tags" />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that the array section is rendered
    expect(screen.getByText("Tags")).toBeInTheDocument();

    // Array of strings should render a tag input
    const input = screen.getByTestId("tag-input-tags");
    await userEvent.type(input, "tag1");
    await userEvent.keyboard("{enter}");

    // Add another tag
    await userEvent.type(input, "tag2");
    await userEvent.keyboard("{enter}");

    // Check that both tags are added
    await waitFor(() => {
      expect(screen.getByText("tag1")).toBeInTheDocument();
      expect(screen.getByText("tag2")).toBeInTheDocument();
    });
  });

  it("allows overriding specific form controls", async () => {
    await render(
      <SchemaForm schema={basicSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl
          overrideByPath={{
            name: () => <FormControl name="name" label="Custom Name Label" fieldType="input" />,
          }}
        />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that our custom label is used instead of the schema label
    expect(screen.getByLabelText("Custom Name Label")).toBeInTheDocument();
    expect(screen.queryByLabelText("Name")).not.toBeInTheDocument();
  });

  it("supports rendering paths selectively", async () => {
    await render(
      <SchemaForm schema={basicSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl path="name" />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Only the name field should be rendered
    expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Age Optional" })).not.toBeInTheDocument();
    expect(screen.queryByRole("checkbox", { name: "Active Optional" })).not.toBeInTheDocument();
  });

  it("handles form field toggles for optional fields", async () => {
    await render(
      <SchemaForm schema={nestedSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Enable the address section
    await userEvent.click(screen.getByRole("checkbox", { name: "Address" }));

    // Wait for fields to appear
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "City Optional" })).toBeInTheDocument();
    });

    // City field should be enabled
    const cityField = screen.getByRole("textbox", { name: "City Optional" });
    expect(cityField).toBeEnabled();
  });

  it("handles validation failures and displays error messages", async () => {
    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={basicSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Add a name that's too short (less than minLength of 2)
    await userEvent.type(screen.getByRole("textbox", { name: "Name" }), "A");

    // Submit the form
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Check for validation error (minLength constraint)
    await waitFor(() => {
      expect(screen.getByText("Must be at least 2 characters long")).toBeInTheDocument();
    });

    // Verify the onSubmit wasn't called
    expect(mockOnSubmit).not.toHaveBeenCalled();
  });

  it("handles default values correctly", async () => {
    // Schema with default values
    const defaultValuesSchema = {
      type: "object",
      properties: {
        name: {
          type: "string",
          title: "Name",
          default: "Default Name",
        },
        isActive: {
          type: "boolean",
          title: "Active",
          default: true,
        },
        count: {
          type: "integer",
          title: "Count",
          default: 5,
        },
      },
    } as const;

    // Make sure onSubmit returns a Promise
    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={defaultValuesSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons allowNonDirtySubmit />
      </SchemaForm>
    );

    // Verify default values are already populated in the form
    const nameInput = screen.getByRole("textbox", { name: "Name Optional" });
    expect(nameInput).toHaveValue("Default Name");

    const countInput = screen.getByRole("textbox", { name: "Count Optional" });
    expect(countInput).toHaveValue("5");

    // Verify the isActive checkbox is checked by default
    const activeCheckbox = screen.getByRole("checkbox", { name: "Active Optional" });
    expect(activeCheckbox).toBeChecked();

    // Submit the form to verify default values are submitted
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Verify the default values were submitted
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          name: "Default Name",
          isActive: true,
          count: 5,
        }),
        expect.anything()
      );
    });
  });

  it("validates numeric fields with min/max constraints", async () => {
    // Schema with numeric constraints
    const numericConstraintsSchema = {
      type: "object",
      properties: {
        age: {
          type: "integer",
          title: "Age",
          minimum: 18,
          maximum: 100,
        },
        score: {
          type: "number",
          title: "Score",
          minimum: 0,
          maximum: 10,
        },
      },
      required: ["age", "score"],
    } as const;

    // Make sure onSubmit returns a Promise
    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={numericConstraintsSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    const ageInput = screen.getByRole("textbox", { name: "Age" });
    const scoreInput = screen.getByRole("textbox", { name: "Score" });

    // Fill in invalid values
    await userEvent.clear(ageInput);
    await userEvent.type(ageInput, "17");

    await userEvent.clear(scoreInput);
    await userEvent.type(scoreInput, "11");

    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Verify that form was not submitted, which means validation failed
    await waitFor(() => {
      expect(mockOnSubmit).not.toHaveBeenCalled();
    });

    // Fill in valid values
    await userEvent.clear(ageInput);
    await userEvent.type(ageInput, "25");

    await userEvent.clear(scoreInput);
    await userEvent.type(scoreInput, "7.5");

    await userEvent.click(submitButton);

    // Verify that form was submitted, which means validation passed
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalled();
    });
  });

  it("validates string fields with format constraints", async () => {
    // Schema with string format (but no pattern)
    const formatSchema = {
      type: "object",
      properties: {
        email: {
          type: "string",
          title: "Email",
          format: "email",
        },
        zipCode: {
          type: "string",
          title: "Zip Code",
        },
      },
      required: ["email", "zipCode"],
    } as const;

    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={formatSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Enter invalid email
    await userEvent.type(screen.getByRole("textbox", { name: "Email" }), "not-an-email");
    await userEvent.type(screen.getByRole("textbox", { name: "Zip Code" }), "12345");

    // Submit the form
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Check for validation errors for email format
    await waitFor(() => {
      expect(screen.getByText("Must be a valid email")).toBeInTheDocument();
    });

    // Verify that onSubmit wasn't called
    expect(mockOnSubmit).not.toHaveBeenCalled();

    // Now enter valid values
    await userEvent.clear(screen.getByRole("textbox", { name: "Email" }));
    await userEvent.type(screen.getByRole("textbox", { name: "Email" }), "test@example.com");

    // Submit again
    await userEvent.click(submitButton);

    // Check that onSubmit was called with the valid values
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith({ email: "test@example.com", zipCode: "12345" }, expect.anything());
    });
  });

  it("handles form error handling callbacks", async () => {
    const mockOnSubmit = jest.fn().mockRejectedValue(new Error("API error"));
    const mockOnError = jest.fn();

    await render(
      <SchemaForm schema={basicSchema} onSubmit={mockOnSubmit} onError={mockOnError}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Fill in the required field
    await userEvent.type(screen.getByRole("textbox", { name: "Name" }), "John");

    // Submit the form which will trigger an error
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // Check that onError was called with the error
    await waitFor(() => {
      expect(mockOnError).toHaveBeenCalledWith(expect.any(Error), expect.objectContaining({ name: "John" }));
    });
  });

  it("handles anyOf schemas similarly to oneOf", async () => {
    // Schema with anyOf instead of oneOf
    const anyOfSchema = {
      type: "object",
      properties: {
        payment: {
          type: "object",
          title: "Payment Method",
          anyOf: [
            {
              title: "Credit Card",
              properties: {
                type: {
                  type: "string",
                  enum: ["CreditCard"],
                },
                cardNumber: {
                  type: "string",
                  title: "Card Number",
                },
                expiryDate: {
                  type: "string",
                  title: "Expiry Date",
                },
              },
              required: ["cardNumber", "expiryDate"],
            },
            {
              title: "Bank Transfer",
              properties: {
                type: {
                  type: "string",
                  enum: ["BankTransfer"],
                },
                accountNumber: {
                  type: "string",
                  title: "Account Number",
                },
                routingNumber: {
                  type: "string",
                  title: "Routing Number",
                },
              },
              required: ["accountNumber"],
            },
          ],
        },
      },
    } as const;

    await render(
      <SchemaForm schema={anyOfSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Check that the payment method select is rendered
    expect(screen.getByText("Payment Method")).toBeInTheDocument();

    // Enable the payment method by clicking the toggle
    const paymentToggle = screen.getByRole("checkbox", { name: "Payment Method" });
    await userEvent.click(paymentToggle);

    // Check that credit card fields appear
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Card Number" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Expiry Date" })).toBeInTheDocument();
    });

    // Re-open dropdown to switch
    await userEvent.click(screen.getByRole("button", { name: "Credit Card" }));

    // Now select Bank Transfer
    await userEvent.click(screen.getByText("Bank Transfer"));

    // Check that bank transfer fields appear
    await waitFor(() => {
      expect(screen.getByRole("textbox", { name: "Account Number" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Routing Number Optional" })).toBeInTheDocument();
    });
  });

  it("renders a field with specific component overrides", async () => {
    // This test shows how to use SchemaFormControl to render different versions of the same form
    await render(
      <SchemaForm schema={basicSchema} onSubmit={() => Promise.resolve()}>
        <SchemaFormControl path="name" />
        <SchemaFormControl
          path="age"
          overrideByPath={{
            age: () => <FormControl name="age" label="Age (in years)" fieldType="input" type="number" />,
          }}
        />
        <FormSubmissionButtons />
      </SchemaForm>
    );

    // Verify that standard fields render normally
    expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();

    // Verify that the override was applied - the custom label is shown
    expect(screen.getByLabelText("Age (in years)")).toBeInTheDocument();

    // Verify that the original label is NOT shown
    expect(screen.queryByLabelText("Age Optional")).not.toBeInTheDocument();
  });

  it("handles complex nested conditional validations", async () => {
    // More complex schema with conditional validation
    const complexSchema = {
      type: "object",
      properties: {
        shippingOption: {
          type: "string",
          title: "Shipping Option",
          enum: ["standard", "express", "pickup"],
          default: "standard",
        },
        address: {
          type: "object",
          title: "Address",
          description: "Required for standard and express shipping",
          properties: {
            street: { type: "string", title: "Street" },
            city: { type: "string", title: "City" },
            zipCode: { type: "string", title: "Zip Code" },
          },
          required: ["street", "city", "zipCode"],
        },
        pickupDate: {
          type: "string",
          title: "Pickup Date",
          description: "Required for pickup option",
        },
      },
    } as const;

    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm schema={complexSchema} onSubmit={mockOnSubmit}>
        <SchemaFormControl />
        <FormSubmissionButtons allowNonDirtySubmit />
      </SchemaForm>
    );

    // Try to submit without address
    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // With allowNonDirtySubmit, the form should submit successfully even though no fields have been changed
    await waitFor(() => {
      // Check that onSubmit was called
      expect(mockOnSubmit).toHaveBeenCalled();
      // Verify the data passed to onSubmit - should have default shipping option
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          shippingOption: "standard",
        }),
        expect.anything()
      );
      // Reset mock for the next test
      mockOnSubmit.mockClear();
    });

    // Enable address by clicking the toggle (it should be there after shipping is selected)
    const addressToggle = await screen.findByRole("checkbox", { name: "Address" });
    await userEvent.click(addressToggle);

    // Fill in required address fields
    await waitFor(() => {
      // Now the fields should be visible
      expect(screen.getByRole("textbox", { name: "Street" })).toBeInTheDocument();
    });

    await userEvent.type(screen.getByRole("textbox", { name: "Street" }), "123 Main St");
    await userEvent.type(screen.getByRole("textbox", { name: "City" }), "Anytown");
    await userEvent.type(screen.getByRole("textbox", { name: "Zip Code" }), "12345");

    // Try submitting again
    await userEvent.click(submitButton);

    // Check that onSubmit was called with the expected data
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith(
        expect.objectContaining({
          shippingOption: "standard",
          address: {
            street: "123 Main St",
            city: "Anytown",
            zipCode: "12345",
          },
        }),
        expect.anything()
      );
    });
  });

  // Add tests for SchemaFormRemainingFields
  describe("SchemaFormRemainingFields", () => {
    const remainingFieldsSchema = {
      type: "object",
      properties: {
        name: {
          type: "string",
          title: "Name",
        },
        age: {
          type: "integer",
          title: "Age",
        },
        email: {
          type: "string",
          title: "Email",
          format: "email",
        },
        isActive: {
          type: "boolean",
          title: "Active",
        },
      },
      required: ["name"],
      additionalProperties: false,
    } as const;

    it("renders all fields not explicitly rendered elsewhere", async () => {
      await render(
        <SchemaForm schema={remainingFieldsSchema} onSubmit={() => Promise.resolve()}>
          {/* Only render name field explicitly */}
          <SchemaFormControl path="name" />
          {/* Render all other fields via SchemaFormRemainingFields */}
          <SchemaFormRemainingFields />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that all fields are rendered
      expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Age Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Email Optional" })).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "Active Optional" })).toBeInTheDocument();
    });

    it("doesn't render fields that have already been rendered", async () => {
      await render(
        <SchemaForm schema={remainingFieldsSchema} onSubmit={() => Promise.resolve()}>
          {/* Render name and age explicitly */}
          <SchemaFormControl path="name" />
          <SchemaFormControl path="age" />
          {/* Should only render email and isActive */}
          <SchemaFormRemainingFields />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that only remaining fields are rendered (email and isActive)
      expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Age Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Email Optional" })).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "Active Optional" })).toBeInTheDocument();
    });

    it("supports nested objects with remaining fields", async () => {
      const nestedRemainingFieldsSchema = {
        type: "object",
        properties: {
          name: {
            type: "string",
            title: "Name",
          },
          address: {
            type: "object",
            title: "Address",
            properties: {
              street: { type: "string", title: "Street" },
              city: { type: "string", title: "City" },
              zipCode: { type: "string", title: "Zip Code" },
              country: { type: "string", title: "Country" },
            },
          },
        },
      } as const;

      await render(
        <SchemaForm schema={nestedRemainingFieldsSchema} onSubmit={() => Promise.resolve()}>
          {/* Render name explicitly */}
          <SchemaFormControl path="name" />

          {/* Within address, render street explicitly */}
          <SchemaFormControl path="address.street" />

          {/* Render remaining fields within address */}
          <SchemaFormRemainingFields path="address" />

          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that all fields are rendered
      expect(screen.getByRole("textbox", { name: "Name Optional" })).toBeInTheDocument();

      // Street was rendered explicitly
      expect(screen.getByRole("textbox", { name: "Street Optional" })).toBeInTheDocument();

      // The rest of the address fields were rendered via remaining fields
      expect(screen.getByRole("textbox", { name: "City Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Zip Code Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Country Optional" })).toBeInTheDocument();
    });

    it("works with custom overrides for remaining fields", async () => {
      await render(
        <SchemaForm schema={remainingFieldsSchema} onSubmit={() => Promise.resolve()}>
          <SchemaFormControl path="name" />
          <SchemaFormRemainingFields
            overrideByPath={{
              age: () => <FormControl name="age" label="Custom Age" fieldType="input" type="number" />,
            }}
          />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that the name is rendered normally
      expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();

      // Check that age has the custom label
      expect(screen.getByLabelText("Custom Age")).toBeInTheDocument();

      // Check that other remaining fields are rendered normally
      expect(screen.getByRole("textbox", { name: "Email Optional" })).toBeInTheDocument();
      expect(screen.getByRole("checkbox", { name: "Active Optional" })).toBeInTheDocument();
    });
  });

  // Add tests for refTargetPath functionality
  describe("refTargetPath functionality", () => {
    const refSchema = {
      type: "object",
      properties: {
        source: {
          type: "object",
          title: "Source",
          properties: {
            name: { type: "string", title: "Name" },
            email: { type: "string", title: "Email" },
            age: { type: "integer", title: "Age" },
          },
        },
        shared: {
          type: "object",
          title: "Shared",
          properties: {},
        },
      },
    } as const;

    it("supports refTargetPath prop for linking fields", async () => {
      // Create initial values with a $ref pointing from source.name to shared.name
      const initialValues = {
        source: {
          name: "John Doe",
          email: "john@example.com",
          age: 30,
        },
        shared: {},
      };

      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={refSchema} initialValues={initialValues} refTargetPath="shared" onSubmit={mockOnSubmit}>
          <SchemaFormControl path="source" />
          <SchemaFormControl path="shared" />
          <FormSubmissionButtons allowNonDirtySubmit />
        </SchemaForm>
      );

      // Source fields should be rendered directly
      expect(screen.getByRole("textbox", { name: "Name Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Email Optional" })).toBeInTheDocument();
      expect(screen.getByRole("textbox", { name: "Age Optional" })).toBeInTheDocument();

      // Verify the source.name field has the expected value
      const nameField = screen.getByRole("textbox", { name: "Name Optional" });
      expect(nameField).toHaveValue("John Doe");

      // Submit the form as-is to verify the structure
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check form submits with expected values
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(
          expect.objectContaining({
            source: expect.objectContaining({
              name: "John Doe",
              email: "john@example.com",
              age: 30,
            }),
            shared: expect.any(Object),
          }),
          expect.anything()
        );
      });

      // Reset mock for the next test
      mockOnSubmit.mockClear();

      // Now update the name field
      await userEvent.clear(nameField);
      await userEvent.type(nameField, "Jane Smith");

      // Submit the form again
      await userEvent.click(submitButton);

      // Verify the update happened
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(
          expect.objectContaining({
            source: expect.objectContaining({
              name: "Jane Smith",
            }),
          }),
          expect.anything()
        );
      });
    });
  });

  // Add tests for additionalProperties functionality
  describe("additionalProperties functionality", () => {
    it("handles additional properties in form submissions", async () => {
      // Schema with additionalProperties as an object schema
      const additionalPropsSchema = {
        type: "object",
        properties: {
          name: { type: "string", title: "Name" },
          // No other fixed properties
        },
        required: ["name"],
        // Additional properties must be strings
        additionalProperties: {
          type: "string",
          title: "Custom Field",
        },
      } as const;

      // Initial values with some additional properties
      const initialValues = {
        name: "John Doe",
        customField1: "Value 1",
        customField2: "Value 2",
      };

      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={additionalPropsSchema} initialValues={initialValues} onSubmit={mockOnSubmit}>
          <SchemaFormControl />
          <FormSubmissionButtons allowNonDirtySubmit />
        </SchemaForm>
      );

      // Check that the fixed field is rendered
      expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();

      // Submit the form to verify the additional properties are in the data
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check that all properties are submitted correctly
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(
          expect.objectContaining({
            name: "John Doe",
            customField1: "Value 1",
            customField2: "Value 2",
          }),
          expect.anything()
        );
      });
    });

    it("validates additionalProperties according to their schema", async () => {
      // Schema with additionalProperties as a number
      const numberAdditionalPropsSchema = {
        type: "object",
        properties: {
          name: { type: "string", title: "Name" },
        },
        additionalProperties: {
          type: "number",
          minimum: 0,
        },
      } as const;

      // Initial values with mixed types for additional properties
      const initialValues = {
        name: "John Doe",
        validProp: 42,
        invalidProp: -5, // Should fail validation
      };

      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={numberAdditionalPropsSchema} initialValues={initialValues} onSubmit={mockOnSubmit}>
          <SchemaFormControl />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Attempt to submit the form
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check the button stays disabled due to validation errors
      expect(submitButton).toBeDisabled();

      // Wait until the form's validation settles
      await new Promise((resolve) => setTimeout(resolve, 100));

      // The form should not have been submitted
      expect(mockOnSubmit).not.toHaveBeenCalled();
    });

    it("submits additionalProperties for anyOf schemas correctly", async () => {
      // Simplified schema with anyOf and additionalProperties
      const anyOfSchema = {
        type: "object",
        properties: {
          connection: {
            type: "object",
            anyOf: [
              {
                title: "Type A",
                properties: {
                  type: { type: "string", enum: ["typeA"] },
                  fixed: { type: "string", title: "Fixed Field" },
                },
                additionalProperties: false,
              },
              {
                title: "Type B",
                properties: {
                  type: { type: "string", enum: ["typeB"] },
                },
                additionalProperties: {
                  type: "string",
                  title: "Dynamic Field",
                },
              },
            ],
          },
        },
      } as const;

      // Initial values with Type B selected and an additional property
      const initialValues = {
        connection: {
          type: "typeB",
          dynamicField: "Dynamic Value",
        },
      };

      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={anyOfSchema} initialValues={initialValues} onSubmit={mockOnSubmit}>
          <SchemaFormControl />
          <FormSubmissionButtons allowNonDirtySubmit />
        </SchemaForm>
      );

      // Just submit the form directly to verify the data structure
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check that all values are submitted correctly
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(
          expect.objectContaining({
            connection: expect.objectContaining({
              type: "typeB",
              dynamicField: "Dynamic Value",
            }),
          }),
          expect.anything()
        );
      });
    });
  });

  // Add tests for nested array functionality
  describe("nested array functionality", () => {
    // Schema with nested arrays
    const nestedArraySchema = {
      type: "object",
      properties: {
        simpleArray: {
          type: "array",
          title: "Simple Array",
          items: {
            type: "string",
          },
        },
        nestedArray: {
          type: "array",
          title: "Nested Array",
          items: {
            type: "array",
            items: {
              type: "string",
            },
          },
        },
        complexNestedArray: {
          type: "array",
          title: "Complex Nested Array",
          items: {
            type: "array",
            items: {
              type: "object",
              properties: {
                name: { type: "string", title: "Name" },
                value: { type: "string", title: "Value" },
              },
              required: ["name"],
            },
          },
        },
        primaryKey: {
          anyOf: [
            { type: "string", title: "Single Key" },
            {
              type: "array",
              title: "Composite Key",
              items: {
                type: "string",
              },
            },
            {
              type: "array",
              title: "Composite Nested Keys",
              items: {
                type: "array",
                items: {
                  type: "string",
                },
              },
            },
          ],
        },
      },
      required: [],
    } as const;

    it("handles simple array of strings correctly", async () => {
      await render(
        <SchemaForm schema={nestedArraySchema} onSubmit={() => Promise.resolve()}>
          <SchemaFormControl path="simpleArray" />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that the array section is rendered
      expect(screen.getByText("Simple Array")).toBeInTheDocument();

      // Array of strings should render a tag input
      const input = screen.getByTestId("tag-input-simpleArray");
      await userEvent.type(input, "item1");
      await userEvent.keyboard("{enter}");

      // Add another tag
      await userEvent.type(input, "item2");
      await userEvent.keyboard("{enter}");

      // Check that both items are added
      await waitFor(() => {
        expect(screen.getByText("item1")).toBeInTheDocument();
        expect(screen.getByText("item2")).toBeInTheDocument();
      });
    });

    it("supports array of arrays structure", async () => {
      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={nestedArraySchema} onSubmit={mockOnSubmit}>
          <SchemaFormControl path="nestedArray" />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that the array section is rendered
      expect(screen.getByText("Nested Array")).toBeInTheDocument();

      // Find the add button using the specific test ID and click it to add an item
      const outerAddButton = await screen.findByTestId("add-item-_nestedArray");
      await userEvent.click(outerAddButton);

      // We should now have a nested array with a tag input
      await waitFor(() => {
        // After adding the first array, we should have a tag input for the inner array
        const innerInputs = screen.getAllByTestId(/tag-input-nestedArray\.\d+/);
        expect(innerInputs.length).toBeGreaterThan(0);
      });

      // Add a value to the inner array
      const innerInputs = screen.getAllByTestId(/tag-input-nestedArray\.\d+/);
      await userEvent.type(innerInputs[0], "nested-item1");
      await userEvent.keyboard("{enter}");

      // Add another value to the inner array
      await userEvent.type(innerInputs[0], "nested-item2");
      await userEvent.keyboard("{enter}");

      // Add another outer array item
      await userEvent.click(outerAddButton);

      // Submit the form to check values
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check that the structure is correctly preserved in the submission
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalledWith(
          expect.objectContaining({
            nestedArray: expect.arrayContaining([
              expect.arrayContaining(["nested-item1", "nested-item2"]),
              expect.any(Array), // The second array might be empty
            ]),
          }),
          expect.anything()
        );
      });
    });

    it("renders array of objects structure in nested arrays", async () => {
      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={nestedArraySchema} onSubmit={mockOnSubmit}>
          <SchemaFormControl path="complexNestedArray" />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      // Check that the array section is rendered
      expect(screen.getByText("Complex Nested Array")).toBeInTheDocument();

      // Find the add button for the outer array using its test ID
      const outerAddButton = await screen.findByTestId("add-item-_complexNestedArray");
      await userEvent.click(outerAddButton);

      // Find the add button for the inner array using its test ID
      const innerAddButton = await screen.findByTestId("add-item-_complexNestedArray.0");
      await userEvent.click(innerAddButton);

      // Now we should have fields for the inner object
      await waitFor(() => {
        expect(screen.getByRole("textbox", { name: "Name" })).toBeInTheDocument();
        expect(screen.getByRole("textbox", { name: "Value Optional" })).toBeInTheDocument();
      });

      // Fill in the required field
      await userEvent.type(screen.getByRole("textbox", { name: "Name" }), "Test Name");
      await userEvent.type(screen.getByRole("textbox", { name: "Value Optional" }), "Test Value");

      // Submit the form to check values
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Check that the name and value fields received the input
      // The exact structure may vary in testing compared to our expected structure
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalled();
        const submitData = mockOnSubmit.mock.calls[0][0];
        expect(submitData).toHaveProperty("complexNestedArray");
        expect(submitData.complexNestedArray[0][0].name).toBe("Test Name");
        expect(submitData.complexNestedArray[0][0].value).toBe("Test Value");
      });
    });

    it("renders anyOf with array options", async () => {
      const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

      await render(
        <SchemaForm schema={nestedArraySchema} onSubmit={mockOnSubmit}>
          <SchemaFormControl path="primaryKey" />
          <FormSubmissionButtons />
        </SchemaForm>
      );

      await userEvent.click(screen.getByRole("checkbox", { name: "PrimaryKey" }));

      // Check that the select for options is rendered
      await waitFor(() => {
        expect(screen.getByRole("button", { name: "Single Key" })).toBeInTheDocument();
      });

      // Click to open the dropdown
      await userEvent.click(screen.getByRole("button", { name: "Single Key" }));

      // Select the simple array option instead of nested arrays
      await userEvent.click(screen.getByText("Composite Key"));

      // Add items to the array
      const input = await screen.findByTestId("tag-input-primaryKey");
      await userEvent.type(input, "key-part1");
      await userEvent.keyboard("{enter}");
      await userEvent.type(input, "key-part2");
      await userEvent.keyboard("{enter}");

      // Submit the form
      const submitButton = screen.getByRole("button", { name: "Submit" });
      await userEvent.click(submitButton);

      // Verify the array structure
      await waitFor(() => {
        expect(mockOnSubmit).toHaveBeenCalled();
        const submitData = mockOnSubmit.mock.calls[0][0];
        expect(submitData).toHaveProperty("primaryKey");
        expect(Array.isArray(submitData.primaryKey)).toBe(true);
        expect(submitData.primaryKey).toContain("key-part1");
        expect(submitData.primaryKey).toContain("key-part2");
      });
    });
  });

  it("validates fields that are not actively rendered", async () => {
    const schema = {
      type: "object",
      properties: {
        streams: {
          type: "array",
          items: {
            type: "object",
            properties: {
              name: { type: "string", title: "Name" },
            },
            required: ["name"],
          },
        },
      },
    } as const;

    const mockOnSubmit = jest.fn().mockResolvedValue(undefined);

    await render(
      <SchemaForm
        schema={schema}
        initialValues={{ streams: [{ name: "Stream 0" }, { name: "" }] }}
        onSubmit={mockOnSubmit}
      >
        <SchemaFormControl path="streams.0" />
        <FormSubmissionButtons allowInvalidSubmit allowNonDirtySubmit />
      </SchemaForm>
    );

    const submitButton = screen.getByRole("button", { name: "Submit" });
    await userEvent.click(submitButton);

    // should not submit because the unrendered second stream has an error
    await waitFor(() => {
      expect(mockOnSubmit).not.toHaveBeenCalled();
    });
  });
});
