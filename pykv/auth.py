# ============================ auth.py ============================
# This file handles:
# 1) Registering new users
# 2) Validating login credentials
# 3) Storing user details in data/users.json
# ================================================================

import json      # Used to store user info in JSON file
import os        # Used for file/directory operations
import hashlib   # Used to hash passwords for security
import re        # Used for email validation using regex

# File path where users will be stored
USERS_FILE = "data/users.json"

# Regex pattern to validate a proper email address
EMAIL_PATTERN = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def _hash_password(password: str) -> str:
    """
    Converts a plain password string into a SHA256 hashed string
    """
    # Convert password to bytes -> hash -> return hex string
    return hashlib.sha256(password.encode()).hexdigest()


def is_valid_email(email: str) -> bool:
    """
    Checks whether input is a proper email address
    """
    # re.match returns match object if pattern matches else None
    return re.match(EMAIL_PATTERN, email) is not None


def is_valid_password(password: str) -> bool:
    """
    Password rule:
    - Minimum 8 characters
    """
    # If length is less than 8 return False (invalid)
    if len(password) < 8:
        return False

    # If rules satisfied return True
    return True


def load_users() -> dict:
    """
    Loads users from users.json
    If file does not exist -> create it
    """
    # Ensure "data" folder exists
    os.makedirs("data", exist_ok=True)

    # If users.json is not there, create a blank JSON object file
    if not os.path.exists(USERS_FILE):
        with open(USERS_FILE, "w") as f:
            json.dump({}, f)

    # Read users.json and return dictionary
    with open(USERS_FILE, "r") as f:
        return json.load(f)


def save_users(users: dict) -> None:
    """
    Saves users dict back into users.json
    """
    # Open file in write mode and dump formatted JSON
    with open(USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)


def register_user(username: str, password: str):
    """
    Registers a new user with restrictions.
    Returns:
        (True, "success message") if OK
        (False, "error message") if not OK
    """
    # Load existing users from file
    users = load_users()

    # Restriction 1: username must be valid email
    if not is_valid_email(username):
        return False, "Enter a valid email address"

    # Restriction 2: password must be minimum 8 characters
    if not is_valid_password(password):
        return False, "Password must be at least 8 characters"

    # Restriction 3: cannot register same email twice
    if username in users:
        return False, "User already exists"

    # Store hashed password (not plain password)
    users[username] = _hash_password(password)

    # Save updated user database
    save_users(users)

    # Return success
    return True, "Registration successful"


def validate_user(username: str, password: str) -> bool:
    """
    Validates login credentials.
    Returns True if valid.
    """
    # Load existing users from JSON
    users = load_users()

    # If email format wrong, deny immediately
    if not is_valid_email(username):
        return False

    # If email not found, deny
    if username not in users:
        return False

    # Convert login password to hash and compare with stored hash
    return users[username] == _hash_password(password)
