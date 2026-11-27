"""Domain errors raised by the service layer and rendered by the API as JSON.

Response body: ``{"detail": "<human message>", "code": "<machine code>"}``.
"""

from typing import ClassVar

from fastapi import status


class AppError(Exception):
    status_code: ClassVar[int] = status.HTTP_400_BAD_REQUEST
    code: ClassVar[str] = "bad_request"
    default_message: ClassVar[str] = "Bad request"

    def __init__(
        self, message: str | None = None, *, headers: dict[str, str] | None = None
    ) -> None:
        self.message = message or self.default_message
        self.headers = headers
        super().__init__(self.message)


class AuthenticationError(AppError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "not_authenticated"
    default_message = "Could not validate credentials"

    def __init__(self, message: str | None = None) -> None:
        super().__init__(message, headers={"WWW-Authenticate": "Bearer"})


class InvalidCredentialsError(AuthenticationError):
    code = "invalid_credentials"
    default_message = "Invalid phone or password"


class OtpRequiredError(AppError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "otp_required"
    default_message = "Two-factor authentication is enabled: a verified login OTP is required"


class InvalidOtpTokenError(AppError):
    status_code = status.HTTP_401_UNAUTHORIZED
    code = "invalid_otp_token"
    default_message = "OTP token is invalid, expired, already used or issued for another purpose"


class PermissionDeniedError(AppError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "permission_denied"
    default_message = "Not enough permissions"


class KycRequiredError(AppError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "KYC_REQUIRED"
    default_message = "Identity verification (KYC) must be approved first"


class UserInactiveError(AppError):
    status_code = status.HTTP_403_FORBIDDEN
    code = "user_inactive"
    default_message = "User is blocked or deleted"


class InvalidPasswordError(AppError):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "invalid_password"
    default_message = "Current password is incorrect"


class InvalidOtpCodeError(AppError):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "invalid_otp_code"
    default_message = "OTP code is invalid or expired"


class NotFoundError(AppError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "not_found"
    default_message = "Resource not found"


class ConflictError(AppError):
    status_code = status.HTTP_409_CONFLICT
    code = "conflict"
    default_message = "Resource already exists"


class InvalidStateError(AppError):
    status_code = status.HTTP_409_CONFLICT
    code = "invalid_state"
    default_message = "Operation is not allowed in the current state"


class OperationInProgressError(AppError):
    status_code = status.HTTP_409_CONFLICT
    code = "OPERATION_IN_PROGRESS"
    default_message = "A request with this idempotency key is being processed, retry later"


class IdempotencyKeyReusedError(AppError):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    code = "IDEMPOTENCY_KEY_REUSED"
    default_message = "This idempotency key was already used for a different request"


class UnprocessableError(AppError):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    code = "unprocessable"
    default_message = "Request cannot be processed"


class CardProcessorRejectedError(AppError):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    code = "CARD_PROCESSOR_REJECTED"
    default_message = "The card processor refused the request"


class PayloadTooLargeError(AppError):
    status_code = status.HTTP_413_CONTENT_TOO_LARGE
    code = "payload_too_large"
    default_message = "Uploaded file is too large"


class UnsupportedMediaTypeError(AppError):
    status_code = status.HTTP_415_UNSUPPORTED_MEDIA_TYPE
    code = "unsupported_media_type"
    default_message = "Unsupported file type"


class TooManyRequestsError(AppError):
    status_code = status.HTTP_429_TOO_MANY_REQUESTS
    code = "too_many_requests"
    default_message = "Too many requests"

    def __init__(self, message: str | None = None, *, retry_after: int) -> None:
        super().__init__(message, headers={"Retry-After": str(max(retry_after, 1))})


class ExternalServiceError(AppError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "external_service_unavailable"
    default_message = "External service is unavailable, try again later"
