from cryptography.fernet import Fernet, InvalidToken
from utilities.log import log


@log
def __decrypt(key: str = None, encrypted_password: str = None) -> str:
    """Decrypt the encrypted password using provided key and returns string type password"""

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
    from utilities.config_parser import get_config_parser
    config = get_config_parser()
    con_str = config.get("MYSQLDB", "MYSQL_AD_CON")
    key = config.get("KEY", "KEY")
    a = __decrypt(key, con_str)
    print(key, con_str, a)

