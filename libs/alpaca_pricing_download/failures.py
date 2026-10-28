"""Scalar failure facts may outlive a response cohort; exceptions may not."""

from dataclasses import dataclass

from .contract import DownloadError, failure_outcome


@dataclass(frozen=True)
class AcquisitionFailure:
    outcome: str
    http_status: int | None
    error: str
    symbol: str | None = None
    provider_message: str | None = None

    @classmethod
    def from_error(cls, error):
        # Transfer only classified scalar failure facts to later cohorts.
        # Retaining the exception would retain its traceback and response buffers;
        # it would not provide stronger evidence about publication or identity.
        return cls(**failure_outcome(error))

    def to_outcome(self):
        value = {"outcome": self.outcome, "http_status": self.http_status, "error": self.error}
        if self.symbol is not None:
            value["symbol"] = self.symbol
        if self.provider_message is not None:
            value["provider_message"] = self.provider_message
        return value

    def exception(self):
        return RetainedAcquisitionError(self)


class RetainedAcquisitionError(DownloadError):
    def __init__(self, failure):
        self.failure = failure
        super().__init__(failure.error)
