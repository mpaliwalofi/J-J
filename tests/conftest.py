# tests/conftest.py
#
# Sets dummy environment variables before any module is imported,
# so module-level Groq/DB clients don't crash during test collection.

import os
os.environ.setdefault("GROQ_API_KEY", "test-dummy-key")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")
