import json
import psycopg2
import redis
from langchain.tools import tool

# Module-level connections intialized at startup
DB_CONN = None
REDIS_CLIENT = None

def init_tools(database_url: str, redis_url: str):
    """
    Initialize database and cache connections for tools.
    """
    global DB_CONN, REDIS_CLIENT
    DB_CONN = psycopg2.connect(database_url)
    DB_CONN.autocommit = True
    REDIS_CLIENT = redis.from_url(redis_url)
    
@tool
def get_user_profile(user_id: str) -> str:
    """
    Retrieve a user's behavior profile including purchase frequency, average cart value, category affinity scores, and recency signals.
    Checks Redis cache first, falls back to PostgreSQL.
    """
    
    # Check redis cache first for low latency access
    cached = REDIS_CLIENT.get(f"profie: {user_id}")
    if cached:
        return cached.decode("utf-8")
    
    # Fall back to PostgreSQL
    cur = DB_CONN.cursor()
    cur.execute(
        "SELECT features FROM user_profiles WHERE user_id = %s", (user_id, )
    )
    
    row = cur.fetchone()
    cur.close()
    
    if row:
        return row[0] if isinstance(row[0], str) else json.dumps(row[0])
    
    return json.dumps({
        "error": "Profile not found"
    })
    
@tool
def get_eligible_offers(cart_value: float) -> str:
    """
    Get all active offers eligible for te given cart value.
    Returns offers where the minimum cart threshold is met.
    """
    cur = DB_CONN.cursor()
    cur.execute(
        """
        SELECT offer_id, name, discount_pct, category, min_cart_value FROM offers_catalog WHERE active = TRUE AND min_cart_value <= %s
        """, (cart_value,)
    )
    rows = cur.fetchall()
    cur.close()
    
    offers = [
        {
            "offer_id": row[0], "name": row[1], "discount_pct": float(row[2]), "category": row[3], "min_cart_value": float(row[4])
        }
        for row in rows
    ]
    return json.dumps(offers)

@tool
def check_business_constraints(offer_id: str, user_id: str) -> str:
    """
    Check if a user is eligible for a specific offer based on business rules like redemption history and frequency caps.
    """
    # Placeholder: in production, will check redemption history and caps
    return json.dumps({
        "eligible": True,
        "reason": "No constraints violated"
    })
    
@tool
def score_relevance(user_profile_json: str, offer_json: str) -> str:
    """
    Compute a relevance score between a user profile and an offer using the dot product of category affinity with offer category.
    """
    profile = json.loads(user_profile_json)
    offer = json.loads(offer_json)
    
    # Extract category affinity from profile
    category_affinity = profile.get("category_affinity", {})
    offer_category = offer.get("category", "")
    
    # Dot product: score is how much the user likes this category
    score = category_affinity.get(offer_category, 0.0)
    return json.dumps({
        "score": round(float(score), 4)
    })
    
    