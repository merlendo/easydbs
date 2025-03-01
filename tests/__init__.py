import platform

def skip_if_arm():
    return platform.machine() == "arm64"
