CREATE TABLE CUSTOMERDIM(
C_CUSTKEY    INTEGER PRIMARY KEY NOT NULL,
C_NAME  TEXT NOT NULL,
N_NAME  TEXT NOT NULL,
R_NAME  TEXT NOT NULL,
C_ADDRESS    TEXT NOT NULL,
C_PHONE      TEXT NOT NULL,
C_ACCTBAL    INTEGER   NOT NULL,
C_BALANCE_CATEGORY TEXT NOT NULL,
C_MKTSEGMENT TEXT NOT NULL,
C_COMMENT    TEXT NOT NULL,
N_COMMENT   TEXT,
R_COMMENT   TEXT
)