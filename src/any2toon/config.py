class Config:
    _warnings_enabled = True

def set_warnings(enabled: bool):
    """
    Enable or disable library warnings.
    
    Args:
        enabled: True to enable warnings, False to disable.
    """
    Config._warnings_enabled = enabled

def warnings_enabled() -> bool:
    """Check if warnings are enabled."""
    return Config._warnings_enabled
