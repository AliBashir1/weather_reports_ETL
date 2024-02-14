from __future__ import annotations

from cryptography.fernet import Fernet, InvalidToken
from src.weather_reports_etl.utilities.config_parser import get_config_parser
from configparser import ConfigParser

def __decrypt(encrypted_password: str = None) -> str:
    """
    The function shall decrypt the encrypted password and return it in string format.
    :param encrypted_password: a string of encrypted password.
    :return: string
    """
    config: ConfigParser = get_config_parser()
    key: str | None = config.get("KEY", "KEY")
    # convert key and encrypted password to bytes object using utf-8 coding
    key: bytes | None = bytes(key, "utf-8")
    encrypted_password: bytes | None = bytes(encrypted_password, "utf-8")

    try:
        cipher_suite: Fernet | None = Fernet(key)
        decrypted_password: bytes = cipher_suite.decrypt(encrypted_password)
        plain_text_password: str = bytes(decrypted_password).decode("utf-8")
    except (InvalidToken, TypeError) as e:
        raise e

    if plain_text_password:
        return plain_text_password
    else:
        raise ValueError("plain_text_password is None type")


if __name__ == "__main__":
    pass
