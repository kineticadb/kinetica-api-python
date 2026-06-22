import platform
from importlib.metadata import version, PackageNotFoundError


def _build_user_agent() -> str:
    try:
        v = version("gpudb")   # reads installed dist metadata
    except PackageNotFoundError:
        v = "0.0.0"
    return (
        f"kinetica-api-python/{v} "
        f"(Python/{platform.python_version()}; "
        f"{platform.system()}/{platform.release()}; "
        f"{platform.machine()})"
    )


USER_AGENT = _build_user_agent()   # computed once at import

