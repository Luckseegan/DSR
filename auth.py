# auth.py
USER_CREDENTIALS = {
    "admin": {"password": "admin123", "access": "full"},
    "legato": {"password": "legato123", "access": "limited"},
    "business": {"password": "business123", "access": "view_only"}
}

def authenticate(username, password):
    user = USER_CREDENTIALS.get(username.lower())
    if user and user["password"] == password:
        return {"authenticated": True, "access_level": user["access"]}
    return {"authenticated": False}