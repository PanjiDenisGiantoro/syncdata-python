import os
from cryptography.fernet import Fernet
from dotenv import dotenv_values
import argparse

ENV_FILE = '.env'

def load_key():
    # Load key from .env or generate one
    config = dotenv_values(ENV_FILE)
    key = config.get("ENCRYPTION_KEY")
    if not key:
        key = Fernet.generate_key().decode()
        print("Generated ENCRYPTION_KEY (add this to your .env):")
        print(f"ENCRYPTION_KEY={key}")
        return None
    return key.encode()

def encrypt_env():
    config = dotenv_values(ENV_FILE)
    key = load_key()
    if key is None:
        return
    cipher = Fernet(key)

    encrypted_lines = []
    for k, v in config.items():
        if k.startswith("DB_PASSWORD"):
            encrypted_value = cipher.encrypt(v.encode()).decode()
            encrypted_lines.append(f"{k}={encrypted_value}")
        else:
            encrypted_lines.append(f"{k}={v}")

    with open(".env.encrypted", "w") as f:
        f.write("\n".join(encrypted_lines))
    print("✅ Encrypted .env written to `.env.encrypted`")

def decrypt_env():
    config = dotenv_values(ENV_FILE)
    key = load_key()
    if key is None:
        return
    cipher = Fernet(key)

    decrypted_lines = []
    for k, v in config.items():
        if k.startswith("DB_PASSWORD"):
            try:
                decrypted_value = cipher.decrypt(v.encode()).decode()
                decrypted_lines.append(f"{k}={decrypted_value}")
            except Exception as e:
                decrypted_lines.append(f"{k}=<Failed to decrypt>")
        else:
            decrypted_lines.append(f"{k}={v}")

    with open(".env.decrypted", "w") as f:
        f.write("\n".join(decrypted_lines))
    print("✅ Decrypted .env written to `.env.decrypted`")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Encrypt/Decrypt .env passwords")
    parser.add_argument("mode", choices=["encrypt", "decrypt"], help="Choose mode")

    args = parser.parse_args()
    if args.mode == "encrypt":
        encrypt_env()
    elif args.mode == "decrypt":
        decrypt_env()
