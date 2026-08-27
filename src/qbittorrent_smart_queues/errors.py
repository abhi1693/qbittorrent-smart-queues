"""Application exceptions shared by API clients and providers."""


class ApiError(RuntimeError):
    """An external API could not provide a valid response."""
