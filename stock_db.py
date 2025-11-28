# stock_db.py
import pymysql
from stock_config import (
    DB_HOST,
    DB_PORT,
    DB_USER,
    DB_PASSWORD,
    DB_NAME,
    PRICE_TABLE_NAME,
    COMPANY_TABLE_NAME,
)


def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def ensure_price_table(conn):
    """
    Stocks 테이블을 생성한다.
    - company_id 는 종목코드 문자열 (VARCHAR(10))
    - Companies(id VARCHAR(10))를 FK로 참조
    """
    create_price_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {PRICE_TABLE_NAME} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        date DATETIME NOT NULL,
        company_id VARCHAR(10) NOT NULL,
        stck_prpr VARCHAR(20) NOT NULL,
        stck_oprc VARCHAR(20),
        stck_hgpr VARCHAR(20),
        stck_lwpr VARCHAR(20),
        acml_vol VARCHAR(30),
        stck_prdy_clpr VARCHAR(20),
        CONSTRAINT fk_{PRICE_TABLE_NAME}_company
            FOREIGN KEY (company_id)
            REFERENCES {COMPANY_TABLE_NAME}(id)
            ON DELETE CASCADE,
        UNIQUE KEY uq_company_date (company_id, date),
        INDEX idx_company_date (company_id, date)
    ) ENGINE=InnoDB
      DEFAULT CHARSET = utf8mb4
      COLLATE = utf8mb4_unicode_ci;
    """

    with conn.cursor() as cur:
        cur.execute(create_price_table_sql)
    conn.commit()
    print(f"✅ 테이블 준비 완료: {PRICE_TABLE_NAME}")


def get_or_create_company(conn, ticker: str, name: str | None = None) -> str:
    """
    기존에는 없으면 INSERT 했지만,
    지금은 Companies를 네가 직접 관리하므로
    - 없으면 에러를 던지고 알려주기만 한다.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT id FROM {COMPANY_TABLE_NAME} WHERE id = %s",
            (ticker,),
        )
        row = cur.fetchone()

    if row:
        return row["id"]

    # 여기서 자동 생성하면 종목/섹터 구성이 꼬일 수 있어서 그냥 에러
    raise RuntimeError(
        f"[{COMPANY_TABLE_NAME}]에 종목코드 {ticker} 가 없습니다. "
        f"먼저 Companies 테이블에 INSERT 해 주세요."
    )


def insert_price_snapshot(conn, snapshot: dict):
    """
    snapshot 예시:
    {
        "date": datetime,
        "company_id": "005930",
        "stck_prpr": "82300",
        "stck_oprc": "81000",
        ...
    }
    """
    sql = f"""
    INSERT INTO {PRICE_TABLE_NAME} (
        date,
        company_id,
        stck_prpr,
        stck_oprc,
        stck_hgpr,
        stck_lwpr,
        acml_vol,
        stck_prdy_clpr
    ) VALUES (
        %(date)s,
        %(company_id)s,
        %(stck_prpr)s,
        %(stck_oprc)s,
        %(stck_hgpr)s,
        %(stck_lwpr)s,
        %(acml_vol)s,
        %(stck_prdy_clpr)s
    )
    ON DUPLICATE KEY UPDATE
        stck_prpr      = VALUES(stck_prpr),
        stck_oprc      = VALUES(stck_oprc),
        stck_hgpr      = VALUES(stck_hgpr),
        stck_lwpr      = VALUES(stck_lwpr),
        acml_vol       = VALUES(acml_vol),
        stck_prdy_clpr = VALUES(stck_prdy_clpr);
    """

    with conn.cursor() as cur:
        cur.execute(sql, snapshot)
