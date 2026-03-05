from flask import Flask
from flask_login import LoginManager
from web.controllers.extraction_controller import extraction_bp
from web.utils.security import User

app = Flask(__name__)
app.secret_key = "db45526b4f57b16a65c3c9c70421101a05161ff16822e32c00c9fcfa0c242ffe"

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "extraction.login"

@login_manager.user_loader
def load_user(user_id):
    return User(user_id)

app.register_blueprint(extraction_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
