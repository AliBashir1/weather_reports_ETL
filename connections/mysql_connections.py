import configparser
import traceback
from sqlalchemy import create_engine, text
import os
from config.definitions import ROOT_DIR


def get_mysql_connections():
    """
    This function initiate a SQLAlchemy engine and returns it
    :return: a Engine type from SQL Alchemy
    """
    config_file_path = os.path.join(ROOT_DIR,"config","conf.ini")
    config_file = os.path.abspath(config_file_path)
    config = configparser.ConfigParser()
    config.read(config_file)

    # Encrypted connection string and key
    con_str = config.get("MYSQLDB", "MYSQL_AD_CON")
    key = config.get("KEY", "KEY")
    # decrypt connection string using key
    d_con_str = __decrypt(key, con_str)

    # Initiate SQL Alchemy Engine.
    engine = create_engine(d_con_str)
    return engine


def __decrypt(key, encrypted_password):
    """
    This function decrypts the password using provided key.
    :param key: an encrypted key
    :param encrypted_password: an encrypted password
    :return: returns a plain text password if decryption is successful.
    """
    from cryptography.fernet import Fernet, InvalidToken
    # # convert key and encrypted password to bytes object using utf-8 coding
    key = bytes(key, "utf-8")
    encrypted_password = bytes(encrypted_password, "utf-8")
    # Initiate the Fernet decoder using the key
    plain_text_password = None
    try:
        cipher_suite = Fernet(key)
        decrypted_password = cipher_suite.decrypt(encrypted_password)
        plain_text_password = bytes(decrypted_password).decode("utf-8")
    except InvalidToken as it:
        print(it, "Invalid token")
    except TypeError as it:
        print(it, traceback.print_exc(), key)
        exit()

    if plain_text_password:
        return plain_text_password
    else:
        print("Error with decrypting the password")


if __name__ == "__main__":
    a = get_mysql_connections()
    print(type(a))
    with a.connect() as con:
        a = con.execute(text("Select * from zipcodes_info;"))
        print(a.fetchall())

