class Any2ToonError(Exception):
    """Base exception for any2toon library."""
    pass

class InvalidFormatError(Any2ToonError):
    """Raised when the input format is invalid or unsupported."""
    pass

class ConversionError(Any2ToonError):
    """Raised when conversion fails (e.g. malformed input)."""
    pass
