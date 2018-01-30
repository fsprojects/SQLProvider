CREATE TABLE IF NOT EXISTS COUNTRIES
(	
	COUNTRY_ID CHAR(2) NOT NULL, 
	COUNTRY_NAME VARCHAR(40), 
	REGION_ID INT, 
	OTHER TEXT, 
	PRIMARY KEY (COUNTRY_ID)
);

CREATE TABLE IF NOT EXISTS LOCATIONS 
(	
	LOCATION_ID INT NOT NULL,
    PRIMARY KEY (LOCATION_ID), 
	STREET_ADDRESS VARCHAR(40), 
	POSTAL_CODE VARCHAR(12), 
	CITY VARCHAR(30) NOT NULL, 
	STATE_PROVINCE VARCHAR(25), 
	COUNTRY_ID CHAR(2),
    FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRIES (COUNTRY_ID)
);


CREATE TABLE IF NOT EXISTS REGIONS
(	
	REGION_ID INT NOT NULL, 
	REGION_NAME VARCHAR(25), 
	REGION_DESCRIPTION TEXT, 
    REGION_ALTERNATE_NAMES VARCHAR(25)[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (REGION_ID)
);


CREATE TABLE IF NOT EXISTS DEPARTMENTS
(	
	 DEPARTMENT_ID INT, 
	 DEPARTMENT_NAME VARCHAR(30) NOT NULL, 
	 MANAGER_ID INT, 
	 LOCATION_ID INT,
	 PRIMARY KEY (DEPARTMENT_ID), 
	 FOREIGN KEY (LOCATION_ID) REFERENCES LOCATIONS (LOCATION_ID)
);

CREATE TABLE IF NOT EXISTS JOBS
(	
	 JOB_ID VARCHAR(10),
     PRIMARY KEY (JOB_ID), 
	 JOB_TITLE VARCHAR(35) NOT NULL, 
     BENEFITS JSONB[],
	 MIN_SALARY DECIMAL, 
	 MAX_SALARY DECIMAL	 
);

CREATE TABLE IF NOT EXISTS EMPLOYEES
(	
    EMPLOYEE_ID INT, 
	FIRST_NAME VARCHAR(20), 
	LAST_NAME VARCHAR(25) NOT NULL, 
	EMAIL VARCHAR(25) NOT NULL, 
	PHONE_NUMBER VARCHAR(20), 
	HIRE_DATE DATE NOT NULL, 
	JOB_ID VARCHAR(10) NOT NULL, 
	SALARY DECIMAL, 
	COMMISSION_PCT DECIMAL, 
	MANAGER_ID INT, 
	DEPARTMENT_ID INT, 
    SALARY_HISTORY DECIMAL[][12],
	CHECK (SALARY > 0), 
	UNIQUE (EMAIL), 
	PRIMARY KEY (EMPLOYEE_ID), 
	FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID), 
	FOREIGN KEY (JOB_ID) REFERENCES JOBS (JOB_ID), 
	FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID)
);

COMMENT ON TABLE EMPLOYEES IS 'Employees is a very important table, used in many places.';
COMMENT ON COLUMN EMPLOYEES.EMAIL IS 'Email is a max-25-char item, having at-sign and a dot';

ALTER TABLE DEPARTMENTS ADD FOREIGN KEY (MANAGER_ID) REFERENCES EMPLOYEES (EMPLOYEE_ID);
 
CREATE TABLE IF NOT EXISTS JOB_HISTORY 
(	
	EMPLOYEE_ID int NOT NULL, 
	START_DATE DATE NOT NULL, 
	END_DATE DATE NOT NULL, 
	JOB_ID VARCHAR(10) NOT NULL, 
	DEPARTMENT_ID int,
	CHECK (end_date > start_date), 
	PRIMARY KEY (EMPLOYEE_ID, START_DATE),
	FOREIGN KEY (JOB_ID) REFERENCES JOBS (JOB_ID), 
	FOREIGN KEY (EMPLOYEE_ID)REFERENCES EMPLOYEES (EMPLOYEE_ID), 
	FOREIGN KEY (DEPARTMENT_ID) REFERENCES DEPARTMENTS (DEPARTMENT_ID)
);

-- Types extracted from PostgreSQL 9.5 and Npgsql 3.0 documentation.
CREATE TABLE IF NOT EXISTS postgresql_types
(
    postgresql_types_id SERIAL NOT NULL,
    --abstime_0 ABSTIME, -- [Npgsql < 3]
    bigint_0 BIGINT, -- alias: INT8
    bigserial_0 BIGSERIAL, -- alias: SERIAL8
    --bit_0 BIT (1), -- [Npgsql < 3]
    bit_0 BIT (10), -- [Npgsql >= 3]
    bit_varying_0 BIT VARYING (10), -- alias: VARBIT [Npgsql >= 3]
    boolean_0 BOOLEAN, -- alias: BOOL
    box_0 BOX,
    bytea_0 BYTEA,
    character_0 CHARACTER (10), -- aliases: CHAR, BPCHAR
    character_varying_0 CHARACTER VARYING (10), -- alias: VARCHAR
    cid_0 CID, -- [Npgsql >= 3]
    cidr_0 CIDR, -- [Npgsql >= 3]
    circle_0 CIRCLE,
    date_0 DATE,
    double_precision_0 DOUBLE PRECISION, -- alias: FLOAT8
    inet_0 INET,
    integer_0 INTEGER, -- aliases: INT, INT4
    internal_char_0 "char", -- [Npgsql >= 3]
    interval_0 INTERVAL,
    json_0 JSON,
    jsonb_0 JSONB,
    line_0 LINE, -- [Npgsql >= 3]
    lseg_0 LSEG,
    macaddr_0 MACADDR,
    money_0 MONEY,
    name_0 NAME,
    numeric_0 NUMERIC (5, 3), -- alias: DECIMAL
    oid_0 OID, -- [Npgsql >= 3]
    --oidvector_0 OIDVECTOR, -- Generates array type which is currently not handled by PostgresProvider
    path_0 PATH,
    --pg_lsn_0 PG_LSN, -- Not yet supported by Npgsql (3.0.7)
    point_0 POINT,
    polygon_0 POLYGON,
    real_0 REAL, -- alias: FLOAT4
    regtype_0 REGTYPE, -- [Npgsql >= 3]
    smallint_0 SMALLINT, -- alias: INT2
    smallserial_0 SMALLSERIAL, -- alias: SERIAL2
    serial_0 SERIAL, -- alias: SERIAL4
    text_0 TEXT,
    time_0 TIME,
    timetz_0 TIME WITH TIME ZONE, -- alias: TIMETZ
    timestamp_0 TIMESTAMP,
    timestamptz_0 TIMESTAMP WITH TIME ZONE, -- alias: TIMESTAMPTZ
    tsquery_0 TSQUERY, -- [Npgsql >= 3]
    tsvector_0 TSVECTOR, -- [Npgsql >= 3]
    --txid_snapshot_0 TXID_SNAPSHOT, -- Not yet supported by Npgsql (3.0.7)
    --unknown_0 UNKNOWN, -- [Npgsql >= 3]
    uuid_0 UUID,
    xid_0 XID, -- [Npgsql >= 3]
    xml_0 XML,
    PRIMARY KEY (postgresql_types_id)
);
