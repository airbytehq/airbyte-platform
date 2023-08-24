import {
  Auth,
  User,
  UserCredential,
  createUserWithEmailAndPassword,
  signInWithEmailAndPassword,
  signInWithEmailLink,
  sendPasswordResetEmail,
  confirmPasswordReset,
  updateProfile,
  applyActionCode,
  sendEmailVerification,
  EmailAuthProvider,
  reauthenticateWithCredential,
  updatePassword,
  AuthErrorCodes,
  signInWithPopup,
  GoogleAuthProvider,
  GithubAuthProvider,
  getIdToken,
  reload,
} from "firebase/auth";

import type { OAuthProviders } from "core/services/auth";
import {
  EmailLinkErrorCodes,
  ResetPasswordConfirmErrorCodes,
  LoginFormErrorCodes,
  SignUpFormErrorCodes,
} from "packages/cloud/services/auth/types";

export class GoogleAuthService {
  constructor(private firebaseAuthProvider: () => Auth) {}

  get auth(): Auth {
    return this.firebaseAuthProvider();
  }

  getCurrentUser(): User | null {
    return this.auth.currentUser;
  }

  async loginWithOAuth(provider: OAuthProviders) {
    // Instantiate the appropriate auth provider. For Google we're specifying the `hd` parameter, to only show
    // Google accounts in the selector that are linked to a business (GSuite) account.
    const authProvider =
      provider === "github" ? new GithubAuthProvider() : new GoogleAuthProvider().setCustomParameters({ hd: "*" });
    await signInWithPopup(this.auth, authProvider);
  }

  async login(email: string, password: string): Promise<UserCredential> {
    return signInWithEmailAndPassword(this.auth, email, password).catch((err) => {
      switch (err.code) {
        case AuthErrorCodes.INVALID_EMAIL:
          throw new Error(LoginFormErrorCodes.EMAIL_INVALID);
        case AuthErrorCodes.USER_CANCELLED:
        case AuthErrorCodes.USER_DISABLED:
          throw new Error(LoginFormErrorCodes.EMAIL_DISABLED);
        case AuthErrorCodes.USER_DELETED:
          throw new Error(LoginFormErrorCodes.EMAIL_NOT_FOUND);
        case AuthErrorCodes.INVALID_PASSWORD:
          throw new Error(LoginFormErrorCodes.PASSWORD_INVALID);
      }

      throw err;
    });
  }

  async signUp(email: string, password: string): Promise<UserCredential> {
    try {
      return await createUserWithEmailAndPassword(this.auth, email, password);
    } catch (err) {
      switch (err.code) {
        case AuthErrorCodes.EMAIL_EXISTS:
          throw new Error(SignUpFormErrorCodes.EMAIL_DUPLICATE);
        case AuthErrorCodes.INVALID_EMAIL:
          throw new Error(SignUpFormErrorCodes.EMAIL_INVALID);
        case AuthErrorCodes.WEAK_PASSWORD:
          throw new Error(SignUpFormErrorCodes.PASSWORD_WEAK);
      }

      throw err;
    }
  }

  async updateProfile(displayName: string): Promise<void> {
    if (this.auth.currentUser === null) {
      throw new Error("Not able to update profile for not loggedIn user!");
    }
    return updateProfile(this.auth.currentUser, { displayName });
  }

  async reauthenticate(email: string, password: string): Promise<UserCredential> {
    if (this.auth.currentUser === null) {
      throw new Error("You must log in first to reauthenticate!");
    }
    const credential = EmailAuthProvider.credential(email, password);
    return reauthenticateWithCredential(this.auth.currentUser, credential);
  }

  async updatePassword(newPassword: string): Promise<void> {
    if (this.auth.currentUser === null) {
      throw new Error("You must log in first to update password!");
    }
    return updatePassword(this.auth.currentUser, newPassword);
  }

  async resetPassword(email: string): Promise<void> {
    return sendPasswordResetEmail(this.auth, email);
  }

  async finishResetPassword(code: string, newPassword: string): Promise<void> {
    try {
      return await confirmPasswordReset(this.auth, code, newPassword);
    } catch (e) {
      switch (e?.code) {
        case AuthErrorCodes.EXPIRED_OOB_CODE:
          throw new Error(ResetPasswordConfirmErrorCodes.LINK_EXPIRED);
        case AuthErrorCodes.INVALID_OOB_CODE:
          throw new Error(ResetPasswordConfirmErrorCodes.LINK_INVALID);
        case AuthErrorCodes.WEAK_PASSWORD:
          throw new Error(ResetPasswordConfirmErrorCodes.PASSWORD_WEAK);
      }

      throw e;
    }
  }

  async sendEmailVerifiedLink(): Promise<void> {
    const currentUser = this.getCurrentUser();

    if (!currentUser) {
      console.error("sendEmailVerifiedLink should be used within auth flow");
      throw new Error("user is not authorised");
    }

    return sendEmailVerification(currentUser);
  }

  async confirmEmailVerify(code: string): Promise<void> {
    await applyActionCode(this.auth, code);

    // Reload the user and get a fresh token with email_verified: true
    if (this.auth.currentUser) {
      await reload(this.auth.currentUser);
      await getIdToken(this.auth.currentUser, true);
    }
  }

  async signInWithEmailLink(email: string): Promise<UserCredential> {
    try {
      return await signInWithEmailLink(this.auth, email);
    } catch (e) {
      switch (e?.code) {
        case AuthErrorCodes.INVALID_EMAIL:
          // The invitation link was sent to a different email
          throw new Error(EmailLinkErrorCodes.EMAIL_MISMATCH);
        case AuthErrorCodes.INVALID_OOB_CODE:
          // The link was already used
          throw new Error(EmailLinkErrorCodes.LINK_INVALID);
        case AuthErrorCodes.EXPIRED_OOB_CODE:
          // The link expired
          throw new Error(EmailLinkErrorCodes.LINK_EXPIRED);
      }

      throw e;
    }
  }

  signOut(): Promise<void> {
    return this.auth.signOut();
  }
}
