import os
import json
import csv
import random
import base64
import binascii
from collections import Counter, defaultdict
from datetime import datetime
from io import BytesIO
import time
import logging
import jwt
import requests
from urllib.parse import quote
from celery import shared_task
import boto3
from botocore.exceptions import ClientError
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig

from dotenv import load_dotenv
import pandas as pd
from astropy.coordinates import SkyCoord
from astropy import units as u
from flask import Flask, jsonify, render_template, request, redirect, url_for, flash, session, send_file, make_response, g
from flask_login import LoginManager, UserMixin, login_user, logout_user, current_user, login_required
from flask_paginate import Pagination, get_page_parameter, get_page_args
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from flask_wtf.csrf import CSRFProtect
from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.utils import secure_filename
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

# Imports from local files
from models import db, User, Transient, Classification
from utils import (
    get_pos, logon,  
    get_most_confident_classification, 
    make_celery, fetch_transient_data,
    get_google_oauth_credentials
)
from vlass_utils import get_vlass_data, run_search

from threading import Thread
from cachetools import TTLCache

#
if os.getenv("FLASK_ENV") == "development":
    load_dotenv(dotenv_path=".env.google")



# Initialize the Kowalski session
kowalski_session = logon()

basedir = os.path.abspath(os.path.dirname(__file__))

# Setup logging for debugging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[
                        logging.FileHandler("debug.log"),
                        logging.StreamHandler()
                    ])
def use_aws_secrets_manager():
    return os.getenv("USE_AWS_SECRETS_MANAGER", "false").lower() == "true"


def get_secrets_manager_client():
    kwargs = {"region_name": os.getenv("AWS_REGION", "us-east-1")}
    return boto3.client("secretsmanager", **kwargs)


_cache = None


def get_secret_cache():
    global _cache
    if _cache is None:
        client = get_secrets_manager_client()
        _cache = SecretCache(SecretCacheConfig(), client)
    return _cache


def get_secret(secret_name):
    if not secret_name:
        return {}
    try:
        cache = get_secret_cache()
        secret_string = cache.get_secret_string(secret_name)
        return json.loads(secret_string)
    except ClientError as exc:
        logging.warning("Unable to read secret %s: %s", secret_name, exc)
    except Exception as exc:
        logging.warning("Unable to parse secret %s: %s", secret_name, exc)
    return {}


# load 
def load_secret_key_from_secrets():
    if not use_aws_secrets_manager():
        return None

    secret_name = os.getenv("AWS_SECRETS_NAME", "")
    if not secret_name:
        return None

    secrets = get_secret(secret_name)
    if not secrets:
        return None

    secret_key = secrets.get("SECRET_KEY") or secrets.get("secret_key")
    if secret_key:
        os.environ["SECRET_KEY"] = secret_key
    return secret_key


secret_key = load_secret_key_from_secrets() or os.getenv("SECRET_KEY", "your_secret_key_here")

# Create flask app instance
class_app = Flask(__name__)
class_app.wsgi_app = ProxyFix(class_app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
class_app.config["SECRET_KEY"] = secret_key
class_app.config["WTF_CSRF_ENABLED"] = os.getenv("WTF_CSRF_ENABLED", "False").lower() == "true"
class_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
class_app.config["PREFERRED_URL_SCHEME"] = os.getenv("PREFERRED_URL_SCHEME", "https")

default_secure_cookie = os.getenv("SESSION_COOKIE_SECURE")
if default_secure_cookie is None:
    default_secure_cookie = os.getenv("FLASK_ENV", "").lower() != "development"
class_app.config["SESSION_COOKIE_SECURE"] = str(default_secure_cookie).lower() == "true"
class_app.config["SESSION_COOKIE_HTTPONLY"] = True
class_app.config["SESSION_COOKIE_SAMESITE"] = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")
class_app.config["SESSION_COOKIE_NAME"] = os.getenv("SESSION_COOKIE_NAME", "session")

remember_cookie_secure = os.getenv("REMEMBER_COOKIE_SECURE")
if remember_cookie_secure is None:
    remember_cookie_secure = default_secure_cookie
class_app.config["REMEMBER_COOKIE_SECURE"] = str(remember_cookie_secure).lower() == "true"
class_app.config["REMEMBER_COOKIE_HTTPONLY"] = True
class_app.config["REMEMBER_COOKIE_SAMESITE"] = os.getenv("SESSION_COOKIE_SAMESITE", "Lax")

# Create Celery instance for background info fetching
class_app.config.update(
    CELERY_BROKER_URL=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    CELERY_RESULT_BACKEND=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
)



celery = make_celery(class_app)

# Create a cache to store prefetched transient data
transient_cache = TTLCache(maxsize=10, ttl=600)

csrf = CSRFProtect(class_app)


def build_database_uri():
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return database_url

    if use_aws_secrets_manager():
        secret_name = os.getenv("AWS_SECRETS_NAME", "")
        if secret_name:
            secrets = get_secret(secret_name)
            if secrets:
                if secrets.get("DATABASE_URL"):
                    os.environ["DATABASE_URL"] = secrets["DATABASE_URL"]
                    return secrets["DATABASE_URL"]

                db_engine = secrets.get("engine") or os.getenv("DB_ENGINE", "postgres")
                db_host = secrets.get("host") or os.getenv("DB_HOST") or os.getenv("RDS_HOSTNAME")
                db_port = secrets.get("port") or os.getenv("DB_PORT") or "5432"
                db_name = secrets.get("dbname") or secrets.get("database") or os.getenv("DB_NAME")
                db_user = secrets.get("username") or os.getenv("DB_USERNAME")
                db_password = secrets.get("password") or os.getenv("DB_PASSWORD")

                if db_host and db_name and db_user and db_password:
                    db_engine = db_engine.lower()
                    if db_engine.startswith("postgres"):
                        db_engine = "postgresql"
                    elif db_engine.startswith("mysql"):
                        db_engine = "mysql+pymysql"

                    uri = f"{db_engine}://{quote(db_user)}:{quote(db_password)}@{db_host}:{db_port}/{db_name}"
                    if os.getenv("DB_SSLMODE", "true").lower() == "true" and db_engine.startswith("postgresql"):
                        uri = f"{uri}?sslmode=require"
                    os.environ["DATABASE_URL"] = uri
                    return uri

    db_host = os.getenv("DB_HOST") or os.getenv("RDS_HOSTNAME")
    if db_host:
        db_engine = os.getenv("DB_ENGINE", "postgres")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("DB_NAME")
        db_user = os.getenv("DB_USERNAME")
        db_password = os.getenv("DB_PASSWORD")
        if db_name and db_user and db_password:
            db_engine = db_engine.lower()
            if db_engine.startswith("postgres"):
                db_engine = "postgresql"
            elif db_engine.startswith("mysql"):
                db_engine = "mysql+pymysql"
            uri = f"{db_engine}://{quote(db_user)}:{quote(db_password)}@{db_host}:{db_port}/{db_name}"
            if os.getenv("DB_SSLMODE", "true").lower() == "true" and db_engine.startswith("postgresql"):
                uri = f"{uri}?sslmode=require"
            os.environ["DATABASE_URL"] = uri
            return uri

    return 'sqlite:///' + os.path.join(basedir, 'class_app.db')


def load_aws_secrets():
    if not use_aws_secrets_manager():
        return
    secret_name = os.getenv("AWS_SECRETS_NAME", "")
    if not secret_name:
        return
    secrets = get_secret(secret_name)
    if not secrets:
        return

    if "SECRET_KEY" in secrets:
        class_app.config["SECRET_KEY"] = secrets["SECRET_KEY"]
    if "client_id" in secrets:
        os.environ["client_id"] = secrets["client_id"]
    if "client_secret" in secrets:
        os.environ["client_secret"] = secrets["client_secret"]


load_secret_key_from_secrets()
load_aws_secrets()
configured_database_uri = build_database_uri()
class_app.config["SQLALCHEMY_DATABASE_URI"] = configured_database_uri
print("[DB DEBUG] configured_database_uri =", configured_database_uri)
print("[DB DEBUG] USE_AWS_SECRETS_MANAGER =", os.getenv("USE_AWS_SECRETS_MANAGER", "false"))
print("[DB DEBUG] AWS_SECRETS_NAME =", os.getenv("AWS_SECRETS_NAME", ""))
print("[DB DEBUG] DB_HOST =", os.getenv("DB_HOST") or os.getenv("RDS_HOSTNAME"))
print("[DB DEBUG] DATABASE_URL env =", os.getenv("DATABASE_URL"))

# Initializing database, and login manager with Flask 
db.init_app(class_app)
print("[DB DEBUG] db.init_app(class_app) completed")
login_manager = LoginManager()
login_manager.init_app(class_app)
# login_manager.login_view = 'login'
login_manager.login_view = None
# Define forms for search, registration, and login
class SearchForm(FlaskForm):
    source_id = StringField('Source ID', validators=[DataRequired()])
    submit = SubmitField('Fetch Data')

# Cache public keys to avoid excessive network requests
key_cache = {}

def get_alb_public_key(region, kid):
    cache_key = f"{region}-{kid}"
    if cache_key in key_cache:
        return key_cache[cache_key]
    url = f"https://public-keys.auth.elb.{region}.amazonaws.com/{kid}"
    pub_key = requests.get(url).text
    key_cache[cache_key] = pub_key
    return pub_key

@login_manager.request_loader
def load_user_from_alb(request):
    encoded_jwt = request.headers.get('X-Amzn-Oidc-Data')
    if not encoded_jwt:
        return None  # Fall back to standard session cookie if header is missing
    
    try:
        # Decode JWT header to find the Key ID (kid) and Region
        jwt_header = jwt.get_unverified_header(encoded_jwt)
        kid = jwt_header['kid']
        region = class_app.config.get('AWS_REGION', 'us-east-1')
        
        pub_key = get_alb_public_key(region, kid)
        
        # Verify signature and expiration (ALB uses ES256 by default)
        data = jwt.decode(encoded_jwt, pub_key, algorithms=['ES256'])
        print(data)
        aws_user_id = data.get('sub')
        email = data.get('email')
        print("[ALB DEBUG] JWT decoded successfully: sub =", aws_user_id, "email =", email)
        # Find or auto-provision the user in your database
        user = User.query.filter_by(email=email).first()
        if not user and email:
            user = User(email=email, username=email)
            db.session.add(user)
            db.session.commit()
            
        return user
    except Exception as e:
        logging.error(f"ALB JWT validation failed: {e}")
        return None

# @login_manager.user_loader
# def load_user(user_id):
#     """Load user by ID."""
#     return User.query.get(int(user_id))

@class_app.context_processor
def inject_search_form():
    """Inject the search form into the context of all templates."""
    return dict(search_form=SearchForm())


@class_app.before_request
def log_request_debug():
    logging.info("[REQUEST DEBUG] method=%s path=%s headers=%s", request.method, request.path, dict(request.headers))


def decode_oidc_token_payload(token):
    """Decode the payload from an unverified JWT-style OIDC token."""
    if not token:
        return None

    token = token.strip()
    if token.startswith("Bearer "):
        token = token[7:].strip()

    parts = token.split(".")
    if len(parts) < 2:
        return None

    payload_segment = parts[1]
    payload_segment += "=" * (-len(payload_segment) % 4)

    try:
        decoded = base64.urlsafe_b64decode(payload_segment.encode("utf-8"))
        return json.loads(decoded.decode("utf-8"))
    except (ValueError, TypeError, binascii.Error, UnicodeDecodeError, json.JSONDecodeError):
        logging.warning("Unable to decode OIDC token payload")
        return None


def get_authenticated_user_identity():
    """Read the authenticated identity from headers or an ALB OIDC token."""
    logging.info("[AUTH DEBUG] path=%s headers=%s", request.path, dict(request.headers))

    email = request.headers.get("X-Forwarded-Email") or request.headers.get("X-User-Email")
    username = request.headers.get("X-Forwarded-User") or request.headers.get("X-User-Name")

    if not email:
        oidc_token = request.headers.get("X-Amzn-Oidc-Data") or request.headers.get("x-amzn-oidc-data")
        if oidc_token:
            claims = decode_oidc_token_payload(oidc_token)
            logging.info("[AUTH DEBUG] decoded OIDC claims=%s", claims)
            if claims:
                email = claims.get("email") or claims.get("sub")
                username = claims.get("preferred_username") or claims.get("username") or claims.get("name")

    if not email:
        return None

    email = str(email).strip().lower()
    if not email:
        return None

    if "@" not in email:
        email = f"{email}@oidc.local"

    if not username:
        username = email.split("@", 1)[0]

    return {
        "email": email,
        "username": username,
        "provider": "load_balancer",
    }


def create_or_get_user_from_identity(identity):
    if not identity:
        return None

    email = identity["email"]
    username = identity["username"]
    provider = identity["provider"]

    try:
        user = User.query.filter_by(email=email).first()
        if not user:
            user = User(
                username=username,
                email=email,
                oauth_provider=provider,
                oauth_id=email,
            )
            db.session.add(user)
            db.session.commit()
        elif not user.oauth_provider or not user.oauth_id:
            user.oauth_provider = provider
            user.oauth_id = email
            db.session.commit()
        return user
    except SQLAlchemyError as exc:
        db.session.rollback()
        logging.exception("Failed to create or update user from load-balancer identity for %s", email)
        return None
    except Exception as exc:
        db.session.rollback()
        logging.exception("Unexpected error while creating/updating user from load-balancer identity for %s", email)
        return None

@class_app.route('/login', methods=['GET'])
def login():
    """Render the login page or redirect an already-authenticated user."""
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    identity = get_authenticated_user_identity()
    if identity:
        try:
            user = create_or_get_user_from_identity(identity)
            if user:
                login_user(user, remember=True)
                flash('Logged in successfully.', 'success')
                return redirect(url_for('index'))
        except Exception:
            logging.exception("Unexpected error during login flow")

    flash('Authentication failed. Please try again.', 'danger')
    return render_template('login.html')


@class_app.route('/authorize')
@class_app.route('/oauth2/idpresponse')
@class_app.route('/login/google/callback')
def authorize():
    """Handle the identity that the load balancer has already authenticated."""
    identity = get_authenticated_user_identity()
    if not identity:
        flash('Authentication failed. Please try again.')
        return redirect(url_for('login'))

    try:
        user = create_or_get_user_from_identity(identity)
        if not user:
            flash('Authentication failed. Please try again.', 'danger')
            return redirect(url_for('login'))

        login_user(user, remember=True)
        flash('Logged in successfully.', 'success')
        return redirect(url_for('index'))
    except Exception:
        logging.exception("Unexpected error during authorize flow")
        flash('Authentication failed. Please try again.', 'danger')
        return redirect(url_for('login'))

@class_app.route('/logout')
@login_required
def logout():
    """Logout current user."""
    logout_user()
    return redirect(url_for('index'))


@class_app.route('/debug_auth')
def debug_auth():
    """Expose auth/session diagnostics for load-balancer-based login troubleshooting."""
    identity = get_authenticated_user_identity()
    oidc_token = request.headers.get("X-Amzn-Oidc-Data") or request.headers.get("x-amzn-oidc-data")
    payload = {
        "is_authenticated": current_user.is_authenticated,
        "user_id": current_user.get_id() if current_user.is_authenticated else None,
        "user_email": current_user.email if current_user.is_authenticated else None,
        "session_keys": sorted(list(session.keys())),
        "identity": identity,
        "oidc_token_present": bool(oidc_token),
        "oidc_claims": decode_oidc_token_payload(oidc_token) if oidc_token else None,
        "headers": {
            "X-Forwarded-Email": request.headers.get("X-Forwarded-Email"),
            "X-User-Email": request.headers.get("X-User-Email"),
            "X-Forwarded-User": request.headers.get("X-Forwarded-User"),
            "X-User-Name": request.headers.get("X-User-Name"),
            "X-Amzn-Oidc-Data": bool(oidc_token),
            "X-Forwarded-Proto": request.headers.get("X-Forwarded-Proto"),
            "X-Forwarded-Host": request.headers.get("X-Forwarded-Host"),
        },
    }
    return jsonify(payload)


@class_app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    """Render the main search form and handle search requests."""
    form = SearchForm()
    if form.validate_on_submit():
        source_id = form.source_id.data.strip()

        if len(source_id) != 12 or source_id[:3].lower() != 'ztf' or not source_id[3:5].isdigit() or not source_id[5:].isalpha():
            flash('Invalid source name.')
            return redirect(url_for('index'))
        
        try:
            # Attempt to redirect to classify_source, this will invoke classify_source route logic
            return redirect(url_for('classify_source', source_id=source_id))
        except TypeError:
            flash('Source does not exist or data could not be retrieved.')
            return redirect(url_for('index'))
        except Exception as e:
            flash(f'An error occurred: {str(e)}')
            return redirect(url_for('index'))
    return render_template('index.html', form=form)


@class_app.route('/classify/<source_id>', methods=['POST'])
@login_required
def classify(source_id):
    """Handle classification of a source by the current user."""
    classification = request.form.get('classification')
    subtype = request.form.get('subtype', None)  # Use None if not provided
    confidence = request.form.get('confidence')

    # Check for missing fields
    if not classification or not confidence:
        flash('Both classification and confidence are required.')
        return redirect(url_for('classify_source', source_id=source_id))

    # Combine classification and subtype if subtype is provided
    existing_classification = Classification.query.filter_by(source_id=source_id, user_id=current_user.id).first()

    classification_text = classification
    if subtype:
        classification_text += f" {subtype}"

    if existing_classification:
        existing_classification.classification = classification_text
        existing_classification.confidence = confidence
        existing_classification.timestamp = datetime.utcnow()
    else:
        new_classification = Classification(
            source_id=source_id,
            user_id=current_user.id,
            classification=classification_text,
            confidence=confidence,
            timestamp=datetime.utcnow()
        )
        db.session.add(new_classification)

    # Save to database
    db.session.commit()
    
    flash(f'Your classification for {source_id} as "{classification_text}" has been recorded.', 'success')
    return redirect(url_for('random_transient'))

@class_app.route('/classify/<source_id>', methods=['GET'])
@login_required
def classify_source(source_id):
    """Render the classification page for a given source."""
    try:
        # Fetch the data for the current transient
        data = fetch_transient_data(kowalski_session, source_id)
        if not data:
            flash('An error occurred while fetching the transient data.')
            return redirect(url_for('index'))

        # The comprehensive raw_alerts data is already prepared in fetch_transient_data()
        # which includes alerts, forced photometry, and previous detections
        raw_alerts = data.get('raw_alerts', [])
        alert_count = data.get('alert_count', 0)
        
        # Debug logs
        logging.debug(f"Source ID: {source_id}")
        logging.debug(f"Alert count reported: {alert_count}")
        logging.debug(f"Number of entries in raw_alerts from fetch_transient_data: {len(raw_alerts)}")
        
        if raw_alerts:
            logging.debug(f"Sample raw_alerts entry: {raw_alerts[0]}")
            logging.debug(f"All JDs in raw_alerts: {[alert.get('jd') for alert in raw_alerts]}")
            
            # Check the origins of the alerts to see data sources
            origins = [alert.get('origin', 'unknown') for alert in raw_alerts]
            logging.debug(f"Alert origins: {Counter(origins)}")

        # Update alert count to match raw_alerts length if needed
        if alert_count != len(raw_alerts):
            logging.debug(f"Updating alert_count from {alert_count} to {len(raw_alerts)}")
            data['alert_count'] = len(raw_alerts)

        # Add VLASS images from session
        data['vlass_images'] = session.pop('vlass_images', [])

        # Render the current transient page
        response = render_template('classify.html', **data)

        # Start prefetching the next transient in a separate thread only if data was fetched (not from cache)
        # This prevents starting a prefetch if we just used cached data.
        user_id = current_user.get_id()
        cached_transient = transient_cache.get(user_id)
        if not (cached_transient and cached_transient.get('status') == 'complete' and cached_transient.get('source_id') == source_id):
            thread = Thread(target=prefetch_transient_data, args=(kowalski_session, user_id, source_id))
            thread.start()

        return response

    except ValueError as e:
        logging.error(f"ValueError: {e}")
        flash('Source does not exist or data could not be retrieved.')
        return redirect(url_for('index'))
    except Exception as e:
        logging.error(f"Exception: {e}")
        import traceback
        logging.error(f"Traceback: {traceback.format_exc()}")
        flash(f'An error occurred: {str(e)}')
        return redirect(url_for('index'))

def prefetch_transient_data(kowalski_session, user_id, last_source_id=None):
    """Prefetch data for the next transient without touching request-scoped Flask state."""
    with class_app.app_context():
        try:
            next_source_id = get_random_id(user_id=user_id, last_source_id=last_source_id)
            if not next_source_id:
                transient_cache[user_id] = {'status': 'empty'}
                return

            prefetched_data = fetch_transient_data(kowalski_session, next_source_id)
            if prefetched_data:
                transient_cache[user_id] = {
                    'data': prefetched_data,
                    'source_id': next_source_id,
                    'status': 'complete'
                }
        except Exception as e:
            logging.error(f"Error while prefetching transient data: {e}")
            transient_cache[user_id] = {'status': 'error'}

@class_app.route('/prefetch_status', methods=['GET'])
@login_required
def prefetch_status():
    user_id = current_user.get_id()
    status = transient_cache.get(user_id, {}).get('status', 'not_started')
    return jsonify({'status': status})

@class_app.route('/retrieve_vlass_data/<source_id>', methods=['POST'])
@login_required
def retrieve_vlass_data(source_id):
    """Retrieve VLASS data for the given source."""
    kowalski_session = logon()
    ra, dec, scat_sep = get_pos(kowalski_session, source_id)
    cutout_dir = os.path.join(basedir, 'static')

    vlass_images_dir = os.path.join(cutout_dir, 'vlass_images')
    search_images = [f'vlass_images/{file_name}' for file_name in os.listdir(vlass_images_dir) if file_name.startswith(source_id) and file_name.endswith(".png")]

    if not search_images:
        get_vlass_data()
        c = SkyCoord(ra, dec, unit='deg')
        run_search(source_id, c)
        search_images = [f'vlass_images/{file_name}' for file_name in os.listdir(vlass_images_dir) if file_name.startswith(source_id) and file_name.endswith(".png")]

    session['vlass_images'] = search_images
    return redirect(url_for('classify_source', source_id=source_id))

def load_transients():
    """Load transients from a CSV file into the database."""
    with class_app.app_context():
        if not Transient.query.first():  # Only load if the table is empty
            BATCH_SIZE = 10000

            with open('transients.csv', newline='', encoding='utf-8-sig') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    transient = Transient(source_id=row[0])
                    db.session.add(transient)
                    BATCH_SIZE -= 1
                    if BATCH_SIZE == 0:
                        db.session.commit()
                        BATCH_SIZE = 10000
                db.session.commit()

def load_test_transients_ids():
    """Load source_id values from test_transients.csv"""
    df = pd.read_csv('test_transients.csv')
    return df['source_id'].tolist()

@class_app.route('/transients', methods=['GET'])
@login_required
def list_transients():
    """List all transients with pagination."""
    page, per_page, offset = get_page_args(
        page_parameter='page', 
        per_page=50  
    )

    transients = Transient.query.offset(offset).limit(per_page).all()
    total = Transient.query.count()

    # Fetch classifications and associated users for each transient
    transients_with_classifications = []
    for transient in transients:
        classifications = Classification.query.filter_by(source_id=transient.source_id).all()
        classified_by_users = [User.query.get(classification.user_id).username for classification in classifications] if classifications else []        
        transients_with_classifications.append({
            'transient': transient,
            'classified_by_users': classified_by_users
        })

    pagination = Pagination(page=page, per_page=per_page, total=total,
                            css_framework='bootstrap5')

    return render_template('transients.html', transients_with_classifications=transients_with_classifications, page=page, per_page=per_page, pagination=pagination)

@class_app.route('/test_transients')
def list_test_transients():
    """List transients from test_transients.csv with pagination."""
    test_transients_ids = load_test_transients_ids()
    
    page, per_page, offset = get_page_args(
        page_parameter='page', 
        per_page=50  
    )
    
    # Query only the transients that are in the test_transients.csv
    transients = Transient.query.filter(Transient.source_id.in_(test_transients_ids)).offset(offset).limit(per_page).all()
    total = Transient.query.filter(Transient.source_id.in_(test_transients_ids)).count()
    
    # Fetch classifications and associated users for each transient
    transients_with_classifications = []
    for transient in transients:
        classifications = Classification.query.filter_by(source_id=transient.source_id).all()
        classified_by_users = [User.query.get(classification.user_id).username for classification in classifications] if classifications else []
        transients_with_classifications.append({
            'transient': transient,
            'classified_by_users': classified_by_users
        })
    
    pagination = Pagination(page=page, per_page=per_page, total=total, css_framework='bootstrap4')
    
    return render_template('test_transients.html', transients_with_classifications=transients_with_classifications, page=page, per_page=per_page, pagination=pagination)


@class_app.route('/export_test_transients', methods=['GET'])
@login_required
def export_test_transients():
    """Export test transients data to Excel."""
    test_transients_ids = load_test_transients_ids()
    
    # Query the transients and their classifications
    transients = Transient.query.filter(Transient.source_id.in_(test_transients_ids)).all()
    
    data = []
    for transient in transients:
        classifications = Classification.query.filter_by(source_id=transient.source_id).all()
        classified_by_users = [User.query.get(classification.user_id).username for classification in classifications]
        most_confident_classification = get_most_confident_classification(classifications)
        
        data.append({
            'source_id': transient.source_id,
            'classified_by': ', '.join(classified_by_users),
            'classification': most_confident_classification
        })

    # Convert to DataFrame (summary)
    df = pd.DataFrame(data)

    # Build per-user classifications sheet
    per_user_rows = []
    for transient in transients:
        classifications = Classification.query.filter_by(source_id=transient.source_id).all()
        for c in classifications:
            user = User.query.get(c.user_id)
            per_user_rows.append({
                'source_id': transient.source_id,
                'username': user.username if user else None,
                'classification': c.classification,
                'confidence': c.confidence,
                'timestamp': c.timestamp.strftime('%Y-%m-%d %H:%M:%S') if c.timestamp else None
            })
    per_user_columns = ['source_id', 'username', 'classification', 'confidence', 'timestamp']
    df_users = pd.DataFrame(per_user_rows, columns=per_user_columns)

    # Create a BytesIO buffer to save the Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Transients')
        df_users.to_excel(writer, index=False, sheet_name='Per-User Classifications')
    
    # Seek to the beginning of the stream
    output.seek(0)

    return send_file(output, as_attachment=True, download_name='test_transients.xlsx', mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

def get_random_id(user_id, last_source_id=None):
    """Return a random, not-yet-classified source_id for the given user.

    Tries, in order:
    1. Any ID the user has not classified yet and that is not the last shown.
    2. Any ID the user has not classified yet.
    3. Any ID that is not the last shown.
    4. Any ID at all (fallback).
    """
    test_transients_ids = load_test_transients_ids()
    if not test_transients_ids:
        logging.warning('No test transients available.')
        return None

    try:
        user_id_int = int(user_id)
    except Exception:
        user_id_int = user_id

    # Find all source_ids this user has already classified
    try:
        classified_ids = {
            row[0]
            for row in db.session.query(Classification.source_id)
            .filter_by(user_id=user_id_int)
            .distinct()
        }
    except Exception:
        classified_ids = set()

    # Build candidate pools with progressive relaxation
    unclassified = [sid for sid in test_transients_ids if sid not in classified_ids]
    unclassified_not_last = [
        sid for sid in unclassified if sid != last_source_id
    ]
    not_last = [
        sid for sid in test_transients_ids if sid != last_source_id
    ]

    if unclassified_not_last:
        candidates = unclassified_not_last
        pool_name = "unclassified_not_last"
    elif unclassified:
        candidates = unclassified
        pool_name = "unclassified"
    elif not_last:
        candidates = not_last
    else:
        candidates = test_transients_ids

    random_source_id = random.choice(candidates)

    return random_source_id

@class_app.route('/random_transient', methods=['GET'])
@login_required
def random_transient():
    """Fetch a random transient, using prefetched data if available."""
    user_id = current_user.get_id()

    # Check if we have prefetched data ready in the cache
    cached_transient = transient_cache.get(user_id) # Use get() instead of pop() here

    if cached_transient and cached_transient.get('status') == 'complete':
        # Use the prefetched data, but never show the same transient twice in a row.
        source_id = cached_transient['source_id']
        last_source_id = session.get("last_random_source_id")

        if source_id == last_source_id:
            # Treat as if there is no usable cache to satisfy UX requirement.
            new_source_id = get_random_id(user_id=user_id, last_source_id=last_source_id)

            # Start prefetching immediately for the following click
            thread = Thread(target=prefetch_transient_data, args=(kowalski_session, user_id, new_source_id))
            thread.start()

            session["last_random_source_id"] = new_source_id
            return redirect(url_for('classify_source', source_id=new_source_id))

        data = cached_transient['data']
        
        # The comprehensive raw_alerts data is already prepared in the cached data
        # which includes alerts, forced photometry, and previous detections
        raw_alerts = data.get('raw_alerts', [])
        alert_count = data.get('alert_count', 0)
        
        # Debug logs
        logging.debug(f"Random transient - Source ID: {source_id}")
        logging.debug(f"Number of entries in raw_alerts from cached data: {len(raw_alerts)}")
        
        if raw_alerts:
            # Check the origins of the alerts to see data sources
            origins = [alert.get('origin', 'unknown') for alert in raw_alerts]
            logging.debug(f"Alert origins: {Counter(origins)}")
        
        # Update alert count to match raw_alerts length if needed
        if alert_count != len(raw_alerts):
            logging.debug(f"Updating alert_count from {alert_count} to {len(raw_alerts)}")
            data['alert_count'] = len(raw_alerts)
        
        # Add VLASS images from session
        data['vlass_images'] = session.pop('vlass_images', [])
        
        # Start prefetching the next transient in a separate thread
        thread = Thread(target=prefetch_transient_data, args=(kowalski_session, user_id, source_id))
        thread.start()

        session["last_random_source_id"] = source_id
        return render_template('classify.html', **data)
      
    else:
        # No valid prefetched data, fetch a new random transient
        logging.debug("No valid prefetched data found. Getting new random ID.")
        new_source_id = get_random_id(user_id=user_id, last_source_id=session.get("last_random_source_id"))
        if not new_source_id:
            flash('No test transients available.', 'danger')
            return redirect(url_for('index'))

        # Start prefetching immediately since we know we need new data
        thread = Thread(target=prefetch_transient_data, args=(kowalski_session, user_id, new_source_id))
        thread.start()
        session["last_random_source_id"] = new_source_id
        return redirect(url_for('classify_source', source_id=new_source_id))

@class_app.route('/user_classifications')
@login_required
def user_classifications():
    """Display a table of user's classifications."""
    user_id = current_user.id
    classifications = Classification.query.filter_by(user_id=user_id).all()
    
    return render_template('user_classifications.html', classifications=classifications, userid=user_id)

@class_app.route('/delete_classification/<int:classification_id>', methods=['POST'])
@login_required
def delete_classification(classification_id):
    """Delete a classification by its ID."""
    classification = Classification.query.get_or_404(classification_id)
    
    # Ensure the classification belongs to the current user
    if classification.user_id != current_user.id:
        flash('You are not authorized to delete this classification.', 'danger')
        return redirect(url_for('user_classifications'))
    
    db.session.delete(classification)
    db.session.commit()
    
    flash('Classification deleted successfully.', 'success')
    return redirect(url_for('user_classifications'))

with class_app.app_context():
    print("[DB DEBUG] running db.create_all() in module startup")
    db.create_all()
    print("[DB DEBUG] db.create_all() completed")
    load_transients()
    
if __name__ == '__main__':
    # Initialize databases and load transients from csv
    with class_app.app_context():
        print("[DB DEBUG] running db.create_all() in __main__")
        db.create_all()
        print("[DB DEBUG] db.create_all() completed in __main__")
        load_transients()
    class_app.run(
        debug=os.getenv("FLASK_DEBUG", "False").lower() == "true",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000"))
    )
