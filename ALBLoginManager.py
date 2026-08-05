import base64
import json
import jwt
import requests
from flask import Flask, abort, current_app, g, request
from functools import wraps


class ALBLoginManager:

  def __init__(self, app=None, region="us-east-1"):
    self.region = region
    self._key_cache = {}  # Cache public keys to minimize AWS API latency
    if app is not None:
      self.init_app(app)

  def init_app(self, app):
    app.extensions["alb_login_manager"] = self
    # Clear cache or handle app teardown context if necessary

  def get_public_key(self, kid):
    """Fetches and caches the AWS public key."""
    if kid not in self._key_cache:
      url = f"https://public-keys.auth.elb.{self.region}.amazonaws.com://{kid}"
      response = requests.get(url, timeout=5)
      response.raise_for_status()
      self._key_cache[kid] = response.text
    return self._key_cache[kid]

  def load_user_from_header(self):
    """Parses, verifies, and extracts user payload from ALB header."""
    encoded_jwt = request.headers.get("X-Amzn-Oidc-Data")
    if not encoded_jwt:
      return None

    try:
      # Step 1: Decode header to find key ID (kid)
      header_part = encoded_jwt.split(".")[0]
      header_part += "=" * (-len(header_part) % 4)
      decoded_header = json.loads(base64.urlsafe_b64decode(header_part))
      kid = decoded_header["kid"]

      # Step 2: Fetch the verified public key
      pub_key = self.get_public_key(kid)

      # Step 3: Decode and verify JWT using ES256
      payload = jwt.decode(encoded_jwt, pub_key, algorithms=["ES256"])
      return payload
    except Exception as e:
      # Log the error in production
      current_app.logger.warning(f"ALB auth verification failed: {e}")
      return None


def alb_login_required(f):
  """Decorator to secure routes."""

  @wraps(f)
  def decorated_function(*args, **kwargs):
    login_manager = current_app.extensions.get("alb_login_manager")
    if not login_manager:
      abort(500, description="ALBLoginManager not initialized on app context")

    user_payload = login_manager.load_user_from_header()
    if not user_payload:
      abort(401, description="Unauthorized: Missing or invalid ALB session")

    # Store user payload globally in Flask context for route usage
    g.current_user = user_payload
    return f(*args, **kwargs)

  return decorated_function