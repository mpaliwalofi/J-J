# utils/validator.py
import re

FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER",
    "TRUNCATE", "CREATE", "GRANT", "REVOKE", "EXECUTE"
]

def validate_sql(query: str) -> tuple[bool, str]:
    clean = " ".join(query.split()).upper()
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf'\b{keyword}\b', clean):
            return False, f"Forbidden operation: {keyword}"
    if not (clean.startswith("SELECT") or clean.startswith("WITH")):
        return False, "Query must start with SELECT or WITH"
    return True, ""
