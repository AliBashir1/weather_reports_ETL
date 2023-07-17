from cryptography.fernet import Fernet, InvalidToken
from src.weather_reports_etl.utilities.log import log


@log
def __decrypt(encrypted_password: str = None) -> str:
    """Decrypt the encrypted password using provided key and returns string type password"""
    # get key
    config = get_config_parser()
    key = config.get("KEY", "KEY")
    # convert key and encrypted password to bytes object using utf-8 coding
    key = bytes(key, "utf-8")
    encrypted_password = bytes(encrypted_password, "utf-8")

    try:
        cipher_suite = Fernet(key)
        decrypted_password = cipher_suite.decrypt(encrypted_password)
        plain_text_password = bytes(decrypted_password).decode("utf-8")
    except (InvalidToken, TypeError) as e:
        raise e

    if plain_text_password:
        return plain_text_password


if __name__ == "__main__":
    from project_files.src.weather_reports_etl.utilities.config_parser import get_config_parser
    config = get_config_parser()
    con_str = config.get("WEATHER_DB", "WEATHER_DB_CONN")
    a = __decrypt(con_str)
    # b = config.get("AWS", "aws_access_key_id")
    print(a)

