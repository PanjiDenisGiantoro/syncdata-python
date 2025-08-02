import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()

key = os.getenv("ENCRYPTION_KEY").encode()
cipher = Fernet(key)

def encrypt(plaintext):
    return cipher.encrypt(plaintext.encode()).decode()

