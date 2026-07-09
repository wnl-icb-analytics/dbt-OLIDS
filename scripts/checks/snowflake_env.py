"""Shared .env loading and Snowflake connection for the checks scripts."""

import os
from pathlib import Path

# scripts/checks/snowflake_env.py -> repo root is two levels up.
REPO_ROOT = Path(__file__).resolve().parents[2]

REQUIRED = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER']


def load_env(path=None):
    """Read KEY=VALUE lines from .env (repo root) merged over os.environ.

    Blank lines and # comments are ignored; surrounding quotes are stripped.
    """
    env = dict(os.environ)
    env_path = Path(path) if path else REPO_ROOT / '.env'
    if env_path.exists():
        for line in env_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, _, value = line.partition('=')
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key:
                env[key] = value
    return env


def get_connection(env):
    """Open a Snowflake connection.

    Password auth when SNOWFLAKE_PASSWORD is set, otherwise external browser SSO.
    """
    import snowflake.connector

    missing = [k for k in REQUIRED if not env.get(k)]
    if missing:
        raise SystemExit(f"Missing environment variables: {', '.join(missing)}")

    params = {'account': env['SNOWFLAKE_ACCOUNT'], 'user': env['SNOWFLAKE_USER']}
    for key, name in [('SNOWFLAKE_ROLE', 'role'),
                      ('SNOWFLAKE_WAREHOUSE', 'warehouse'),
                      ('SNOWFLAKE_TARGET_DATABASE', 'database')]:
        if env.get(key):
            params[name] = env[key]

    password = env.get('SNOWFLAKE_PASSWORD')
    authenticator = env.get('SNOWFLAKE_AUTHENTICATOR')
    if password:
        params['password'] = password
        if authenticator:
            params['authenticator'] = authenticator
    else:
        params['authenticator'] = authenticator or 'externalbrowser'

    return snowflake.connector.connect(**params)
