import os
from urllib.parse import quote_plus

from superset.app import create_app
from superset import db, security_manager


# ============================================================
# CREATE SUPERSET APPLICATION
# ============================================================

app = create_app()


# ============================================================
# RUN INSIDE FLASK APPLICATION CONTEXT
# ============================================================

with app.app_context():

    # Import Superset models only AFTER application context exists.
    from superset.models.core import Database

    # ========================================================
    # 1. CREATE ADMIN USER
    # ========================================================

    username = os.environ["SUPERSET_ADMIN_USERNAME"]

    if not security_manager.find_user(username=username):

        role = security_manager.find_role("Admin")

        if role is None:
            role = security_manager.add_role("Admin")

        user = security_manager.add_user(
            username=username,
            first_name="Lakehouse",
            last_name="Admin",
            email="admin@example.local",
            role=role,
            password=os.environ["SUPERSET_ADMIN_PASSWORD"],
        )

        if not user:
            raise RuntimeError(
                "Could not create Superset administrator"
            )

    # ========================================================
    # 2. CREATE / UPDATE VIETNAM LAKEHOUSE DATABASE
    # ========================================================

    database = (
        db.session
        .query(Database)
        .filter_by(database_name="Vietnam Lakehouse")
        .one_or_none()
    )

    if database is None:
        database = Database(
            database_name="Vietnam Lakehouse"
        )

        db.session.add(database)

    # ========================================================
    # 3. CONFIGURE READ-ONLY POSTGRES CONNECTION
    # ========================================================

    database.sqlalchemy_uri = (
        "postgresql+psycopg2://bi_reader:"
        + quote_plus(os.environ["BI_DB_PASSWORD"])
        + "@postgres:5432/lakehouse"
    )

    database.expose_in_sqllab = True
    database.allow_dml = False

    # ========================================================
    # 4. SAVE CHANGES
    # ========================================================

    db.session.commit()


print(
    "Superset administrator and Vietnam Lakehouse "
    "connection are ready."
)