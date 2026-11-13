"""Small database helpers shared by the pipeline phases."""
import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def to_py(v):
    """Convert pandas/numpy values to plain Python types DB drivers accept."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if isinstance(v, np.generic):
        return v.item()
    return v


def read_df(hook, sql: str, parameters=None) -> pd.DataFrame:
    """Run a SELECT through the driver's cursor and return a DataFrame.

    Avoids pandas.read_sql on a raw DBAPI connection, which pandas warns
    is unsupported for MySQL/PostgreSQL drivers.
    """
    conn = hook.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, parameters)
        columns = [d[0] for d in cur.description]
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return pd.DataFrame(list(rows), columns=columns)


def df_to_rows(df: pd.DataFrame, columns: list) -> list:
    return [tuple(to_py(v) for v in rec) for rec in df[columns].itertuples(index=False, name=None)]


def write_tables_atomically(hook, writes: list, quote: str = "`", batch_size: int = 5000) -> None:
    """Write several tables in ONE transaction - all succeed or none change.

    Each item in `writes` is a dict:
      table, df, columns, and optionally delete_sql / delete_params
      (statement run first to remove the rows being replaced).
    """
    conn = hook.get_conn()
    hook.set_autocommit(conn, False)
    cur = conn.cursor()
    try:
        for w in writes:
            if w.get("delete_sql"):
                cur.execute(w["delete_sql"], w.get("delete_params"))
            cols = w["columns"]
            sql = (
                f"INSERT INTO {w['table']} ({', '.join(f'{quote}{c}{quote}' for c in cols)}) "
                f"VALUES ({', '.join(['%s'] * len(cols))})"
            )
            rows = df_to_rows(w["df"], cols)
            for start in range(0, len(rows), batch_size):
                cur.executemany(sql, rows[start:start + batch_size])
            log.info("Wrote %s rows to %s", len(rows), w["table"])
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Write failed - transaction rolled back, no table changed.")
        raise
    finally:
        cur.close()
        conn.close()
