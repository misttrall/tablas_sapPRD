from flask_login import UserMixin

USERS = {
    "ahurtado@invertec.cl": "Invertec.26",
    "admin": "sap2024"
}

class User(UserMixin):
    def __init__(self, username):
        self.id = username
