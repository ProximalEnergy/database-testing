import os


def get_remote() -> bool:
    print(os.getcwd())
    return "marcusmarosvari" not in os.getcwd()
