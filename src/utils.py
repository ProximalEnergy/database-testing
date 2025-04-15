import os


def get_remote() -> bool:
    return "marcusmarosvari" not in os.getcwd()
