import os
from urllib.parse import quote_plus

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    "postgresql+psycopg2://superset:"
    + quote_plus(os.environ["SUPERSET_DB_PASSWORD"])
    + "@postgres:5432/superset"
)
WTF_CSRF_ENABLED = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SQLALCHEMY_TRACK_MODIFICATIONS = False
