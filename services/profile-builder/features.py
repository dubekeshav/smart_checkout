import json
from datetime import datetime, timezone
from typing import Dict, Any

async def compute_features(user_id: str, event: dict, db_conn: Any) -> Dict[str, Any]:
    '''
    Compute real time features for a user from their event history.
    
    purchase_frequency: how often the user starts a checkout relative to their active days.

    avg_cart_value: the average dollar value of items added to cart.

    category_affinity: a normalized dictionary showing which product categories the user browses most.

    recency_score: a decay signal that is higher when the user was active recently.

    session_depth: how many unique sessions the user has had, indicating engagement depth.
    '''
    
    # Querying all events for this user from the events table
    rows = await db_conn.fetch(
        "SELECT event_type, payload, created_at FROM events WHERE user_id = $1 ORDER BY created_at", user_id
    )
    
    if not rows:
        return {
            'user_id': user_id,
            'purchase_frequency': 0.0,
            'avg_cart_value': 0.0,
            'category_affinity': {},
            'recency_score': 0.0,
            'session_depth': 0
        }
        
    # Purchase frequency: checkout_start count / Days active
    checkout_count = sum(1 for row in rows if row['event_type'] == 'checkout_start')
    
    timestamps = [row['created_at'] for row in rows]
    
    days_active = max((max(timestamps) - min(timestamps)).days, 1) # Avoid division by zero
    
    purchase_frequency = checkout_count / days_active
    
    # Average Cart value: mean of add_to_cart payload.cart_value
    cart_values = []
    for row in rows:
        if row['event_type'] == 'add_to_cart':
            payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            
            if 'cart_value' in payload:
                cart_values.append(float(payload['cart_value']))
    
    avg_cart_value = sum(cart_values) / len(cart_values) if cart_values else 0.0
    
    # Category affinity: normalized score from page_view events
    category_counts = {}
    for row in rows:
        if row['event_type'] == 'page_view':
            payload = json.loads(row['payload']) if isinstance(row['payload'], str) else row['payload']
            
            category = payload.get('category', 'unknown')
            category_counts = category_counts.get(category, 0) + 1
            
    total_views = sum(category_counts.values()) if category_counts else 1
    category_affinity = {k: v/total_views for k, v in category_counts.items()}
    
    # Recency score: 1 / (1 + days since last event)
    last_event_time = max(timestamps)
    days_since_last = (datetime.now(timezone.utc) - last_event_time).days
    recency_score = 1 / (1 + days_since_last)
    
    # Session depth: unique session count (assuming session_id in payload)
    session_ids = set()
    for row in rows:
        payload = json.loads(row["payload"]) if isinstance(row['payload'], str) else row['payload']
        if 'session_id' in payload:
            session_ids.add(payload['session_id'])
            
    session_depth = len(session_ids) if session_ids else 1
    
    return {
        'user_id': user_id,
        'purchase_frequency': round(purchase_frequency, 4),
        'avg_cart_value': round(avg_cart_value, 2),
        'category_affinity': category_affinity,
        'recency_score': round(recency_score, 4),
        'session_depth': session_depth
    }