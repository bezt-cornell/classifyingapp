from flask import Flask, g
from ALBLoginManager import ALBLoginManager, alb_login_required

app = Flask(__name__)

# Initialize the custom manager
alb_manager = ALBLoginManager(app, region="us-east-1")


@app.route("/dashboard")
@alb_login_required
def dashboard():
  # Access authenticated claims safely from flask.g
  user_email = g.current_user.get("email")
  user_id = g.current_user.get("sub")

  return {
      "message": "Welcome to your secure dashboard",
      "user_id": user_id,
      "email": user_email,
  }


if __name__ == "__main__":
  app.run()