export enum CloudRoutes {
  Root = "/",
  AcceptInvitation = "/accept-invite",
  Metrics = "metrics",

  // Auth routes
  Signup = "/signup",
  Login = "/login",
  Sso = "/sso",
  SsoBookmark = "/sso/:companyIdentifier",
}
