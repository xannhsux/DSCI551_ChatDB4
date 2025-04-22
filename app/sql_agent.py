import sqlite3
import os
import logging
from sqlalchemy import create_engine

# ——— Logging configuration ———
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ——— Database file paths ———
SQLITE_DB_DIR    = os.environ.get("SQLITE_DB_DIR", os.path.join(os.getcwd(), "data"))
LOCATION_DB_PATH = os.path.join(SQLITE_DB_DIR, "hotel_location.db")
RATE_DB_PATH     = os.path.join(SQLITE_DB_DIR, "hotel_rate.db")

# Optional SQLAlchemy engines
location_engine = create_engine(f"sqlite:///{LOCATION_DB_PATH}")
rate_engine     = create_engine(f"sqlite:///{RATE_DB_PATH}")

def get_connection(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def execute_sql_query(db_path, sql, params=(), fetch_all=True):
    conn = get_connection(db_path)
    cur  = conn.cursor()
    cur.execute(sql, params)
    rows = cur.fetchall() if fetch_all else cur.fetchone()
    conn.commit()
    conn.close()
    return rows

def create_views():
    try:
        # 1) hotel_complete_view in hotel_location.db
        execute_sql_query(LOCATION_DB_PATH, "DROP VIEW IF EXISTS hotel_complete_view;")
        execute_sql_query(LOCATION_DB_PATH, """
            CREATE VIEW hotel_complete_view AS
              SELECT h1.ID,       h1.hotel_name, l.county, l.state
                FROM hotel_name1 h1 JOIN location l ON h1.ID = l.ID
              UNION ALL
              SELECT h2.ID,       h2.hotel_name, l.county, l.state
                FROM hotel_name2 h2 JOIN location l ON h2.ID = l.ID
              UNION ALL
              SELECT h3.ID,       h3.hotel_name, l.county, l.state
                FROM hotel_name3 h3 JOIN location l ON h3.ID = l.ID;
        """)
        logger.info("Dropped & recreated hotel_complete_view")

        # 2) unified_hotel & rate_complete_view in hotel_rate.db
        conn = sqlite3.connect(RATE_DB_PATH); conn.row_factory = sqlite3.Row; cur = conn.cursor()
        cur.execute("DROP VIEW IF EXISTS unified_hotel;")
        cur.execute("DROP VIEW IF EXISTS rate_complete_view;")

        cur.execute("""
            CREATE VIEW unified_hotel AS
              SELECT ID, hotel_name FROM hotel_name1
              UNION ALL
              SELECT ID, hotel_name FROM hotel_name2
              UNION ALL
              SELECT ID, hotel_name FROM hotel_name3;
        """)

        cur.execute("""
            CREATE VIEW rate_complete_view AS
            SELECT
              r.ID,
              u.hotel_name,
              r.rating,
              r.service,
              r.rooms,
              r.cleanliness
            FROM rate r
            LEFT JOIN unified_hotel u ON r.ID = u.ID;
        """)

        conn.commit(); conn.close()
        logger.info("Dropped & recreated unified_hotel and rate_complete_view")
        return True

    except Exception as e:
        logger.error(f"Error creating views: {e}")
        return False

# create views at startup
if not create_views():
    logger.warning("View creation failed; falling back to manual queries")


# ——— Public query functions ———

def get_all_hotels():
    return execute_sql_query(
        LOCATION_DB_PATH,
        "SELECT DISTINCT ID, hotel_name, county, state FROM hotel_complete_view"
    )

def get_all_reviews():
    return execute_sql_query(RATE_DB_PATH, "SELECT * FROM rate_complete_view")

def get_reviews_by_county(county):
    hotels = execute_sql_query(
        LOCATION_DB_PATH,
        "SELECT ID FROM hotel_complete_view WHERE county = ?",
        (county,)
    )
    ids = [h["ID"] for h in hotels]
    if not ids: return []
    ph = ",".join("?"*len(ids))
    return execute_sql_query(RATE_DB_PATH, f"SELECT * FROM rate_complete_view WHERE ID IN ({ph})", ids)

def get_reviews_by_state(state):
    hotels = execute_sql_query(
        LOCATION_DB_PATH,
        "SELECT ID FROM hotel_complete_view WHERE UPPER(state)=UPPER(?)",
        (state,)
    )
    ids = [h["ID"] for h in hotels]
    if not ids: return []
    ph = ",".join("?"*len(ids))
    return execute_sql_query(RATE_DB_PATH, f"SELECT * FROM rate_complete_view WHERE ID IN ({ph})", ids)

def find_hotels_with_min_rating(min_rating):
    return execute_sql_query(
        RATE_DB_PATH,
        "SELECT * FROM rate_complete_view WHERE rating >= ?",
        (min_rating,)
    )

def execute_custom_query(query, is_location_db=True):
    db = LOCATION_DB_PATH if is_location_db else RATE_DB_PATH
    return execute_sql_query(db, query)
