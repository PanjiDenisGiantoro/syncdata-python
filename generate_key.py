from cryptography.fernet import Fernet

key = Fernet.generate_key()
print("ENCRYPTION_KEY =", key.decode())
