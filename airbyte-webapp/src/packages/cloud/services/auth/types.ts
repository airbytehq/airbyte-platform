export const enum SignUpFormErrorCodes {
  EMAIL_DUPLICATE = "email.duplicate",
  PASSWORD_WEAK = "password.weak",
  EMAIL_INVALID = "email.invalid",
}

export const enum LoginFormErrorCodes {
  EMAIL_NOT_FOUND = "email.notfound",
  EMAIL_DISABLED = "email.disabled",
  PASSWORD_INVALID = "password.invalid",
  EMAIL_INVALID = "email.invalid",
}

export const enum ResetPasswordConfirmErrorCodes {
  LINK_EXPIRED = "link.expired",
  LINK_INVALID = "link.invalid",
  PASSWORD_WEAK = "password.weak",
}

export const enum EmailLinkErrorCodes {
  EMAIL_MISMATCH = "email.mismatch",
  LINK_EXPIRED = "link.expired",
  LINK_INVALID = "link.invalid",
}
