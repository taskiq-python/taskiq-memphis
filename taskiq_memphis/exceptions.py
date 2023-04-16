from taskiq.exceptions import BrokerError


class BaseTaskiqMemphisError(BrokerError):
    """Base error for all Taskiq-Memphis error."""


class TooLateConfigurationError(BaseTaskiqMemphisError):
    """Error if there is a try to configure memphis after a startup."""


class StartupNotCalledError(BaseTaskiqMemphisError):
    """Error if startup wasn't called."""
