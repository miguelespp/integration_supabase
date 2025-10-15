import os
import sys
from typing import List, Dict, Optional

try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except Exception:
    DOTENV_AVAILABLE = False

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except Exception:  # pragma: no cover - import-time
    psycopg2 = None  # type: ignore


def _load_env(dotenv_path: Optional[str] = None) -> None:
    if DOTENV_AVAILABLE:
        load_dotenv(dotenv_path)


def get_required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise EnvironmentError(f"Variable de entorno obligatoria no encontrada: {name}")
    return val


def connect_db():
    """Conecta a la base de datos PostgreSQL usando psycopg2 y variables de entorno.

    Variables requeridas: PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no está disponible. Instala psycopg2-binary en tu entorno.")

    _load_env()

    host = get_required_env("PGHOST")
    port = get_required_env("PGPORT")
    user = get_required_env("PGUSER")
    password = get_required_env("PGPASSWORD")
    dbname = get_required_env("PGDATABASE")

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    return conn


def list_tables(conn) -> List[Dict[str, str]]:
    """Devuelve lista de tablas (schema, table_name) únicamente del esquema public."""
    q = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_type = 'BASE TABLE'
      AND table_schema = 'public'
    ORDER BY table_name;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q)
        return cur.fetchall()


def get_table_columns(conn, schema: str, table: str) -> List[Dict[str, str]]:
    q = """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position;
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, (schema, table))
        return cur.fetchall()


def extract_schema_info() -> Dict[str, List[Dict[str, str]]]:
    """Conecta y extrae las tablas y sus columnas. Retorna un dict donde la clave es 'schema.table'."""
    conn = connect_db()
    try:
        tables = list_tables(conn)
        result = {}
        for t in tables:
            schema = t["table_schema"]
            name = t["table_name"]
            cols = get_table_columns(conn, schema, name)
            result[f"{schema}.{name}"] = cols
        return result
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        info = extract_schema_info()
        for table, cols in info.items():
            print(f"Tabla: {table}")
            for c in cols:
                print(f"  - {c['column_name']} ({c['data_type']}) nullable={c['is_nullable']}")
            print()
    except EnvironmentError as e:
        print(f"❌ Error de entorno: {e}")
        sys.exit(1)
    except RuntimeError as e:
        print(f"❌ Runtime error: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        sys.exit(3)
