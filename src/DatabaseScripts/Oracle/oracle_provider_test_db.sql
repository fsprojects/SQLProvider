CREATE TABLE  "REGIONS" 
   (    "REGION_ID" NUMBER CONSTRAINT "REGION_ID_NN" NOT NULL ENABLE, 
    "REGION_NAME" VARCHAR2(25), 
     CONSTRAINT "REG_ID_PK" PRIMARY KEY ("REGION_ID") ENABLE
   ) ;
   
   CREATE TABLE  "COUNTRIES" 
   (    "COUNTRY_ID" CHAR(2) CONSTRAINT "COUNTRY_ID_NN" NOT NULL ENABLE, 
    "COUNTRY_NAME" VARCHAR2(40), 
    "REGION_ID" NUMBER, 
     CONSTRAINT "COUNTRY_C_ID_PK" PRIMARY KEY ("COUNTRY_ID") ENABLE
   ) ORGANIZATION INDEX NOCOMPRESS ;
   
   CREATE TABLE  "JOBS" 
   (    "JOB_ID" VARCHAR2(10), 
    "JOB_TITLE" VARCHAR2(35) CONSTRAINT "JOB_TITLE_NN" NOT NULL ENABLE, 
    "MIN_SALARY" NUMBER(6,0), 
    "MAX_SALARY" NUMBER(6,0), 
     CONSTRAINT "JOB_ID_PK" PRIMARY KEY ("JOB_ID") ENABLE
   ) ;
   
   CREATE TABLE  "EMPLOYEES" 
   (    "EMPLOYEE_ID" NUMBER(6,0), 
    "FIRST_NAME" VARCHAR2(20), 
    "LAST_NAME" VARCHAR2(25) CONSTRAINT "EMP_LAST_NAME_NN" NOT NULL ENABLE, 
    "EMAIL" VARCHAR2(25) CONSTRAINT "EMP_EMAIL_NN" NOT NULL ENABLE, 
    "PHONE_NUMBER" VARCHAR2(20), 
    "HIRE_DATE" DATE CONSTRAINT "EMP_HIRE_DATE_NN" NOT NULL ENABLE, 
    "JOB_ID" VARCHAR2(10) CONSTRAINT "EMP_JOB_NN" NOT NULL ENABLE, 
    "SALARY" NUMBER(8,2), 
    "COMMISSION_PCT" NUMBER(2,2), 
    "MANAGER_ID" NUMBER(6,0), 
    "DEPARTMENT_ID" NUMBER(4,0), 
     CONSTRAINT "EMP_SALARY_MIN" CHECK (salary > 0) ENABLE, 
     CONSTRAINT "EMP_EMAIL_UK" UNIQUE ("EMAIL") ENABLE, 
     CONSTRAINT "EMP_EMP_ID_PK" PRIMARY KEY ("EMPLOYEE_ID") ENABLE
   ) ;
   COMMENT ON TABLE "EMPLOYEES" IS 'Employees is a very important table, used in many places.';
   COMMENT ON COLUMN EMPLOYEES.EMAIL IS 'Email is a max-25-char item, having at-sign and a dot';
   
   CREATE TABLE  "LOCATIONS" 
   (    "LOCATION_ID" NUMBER(4,0), 
    "STREET_ADDRESS" VARCHAR2(40), 
    "POSTAL_CODE" VARCHAR2(12), 
    "CITY" VARCHAR2(30) CONSTRAINT "LOC_CITY_NN" NOT NULL ENABLE, 
    "STATE_PROVINCE" VARCHAR2(25), 
    "COUNTRY_ID" CHAR(2), 
     CONSTRAINT "LOC_ID_PK" PRIMARY KEY ("LOCATION_ID") ENABLE
   ) ;
   
   CREATE TABLE  "DEPARTMENTS" 
   (    "DEPARTMENT_ID" NUMBER(4,0), 
    "DEPARTMENT_NAME" VARCHAR2(30) CONSTRAINT "DEPT_NAME_NN" NOT NULL ENABLE, 
    "MANAGER_ID" NUMBER(6,0), 
    "LOCATION_ID" NUMBER(4,0), 
     CONSTRAINT "DEPT_ID_PK" PRIMARY KEY ("DEPARTMENT_ID") ENABLE
   ) ;CREATE TABLE  "JOB_HISTORY" 
   (    "EMPLOYEE_ID" NUMBER(6,0) CONSTRAINT "JHIST_EMPLOYEE_NN" NOT NULL ENABLE, 
    "START_DATE" DATE CONSTRAINT "JHIST_START_DATE_NN" NOT NULL ENABLE, 
    "END_DATE" DATE CONSTRAINT "JHIST_END_DATE_NN" NOT NULL ENABLE, 
    "JOB_ID" VARCHAR2(10) CONSTRAINT "JHIST_JOB_NN" NOT NULL ENABLE, 
    "DEPARTMENT_ID" NUMBER(4,0), 
     CONSTRAINT "JHIST_DATE_INTERVAL" CHECK (end_date > start_date) ENABLE, 
     CONSTRAINT "JHIST_EMP_ID_ST_DATE_PK" PRIMARY KEY ("EMPLOYEE_ID", "START_DATE") ENABLE
   ) ;ALTER TABLE  "COUNTRIES" ADD CONSTRAINT "COUNTR_REG_FK" FOREIGN KEY ("REGION_ID")
      REFERENCES  "REGIONS" ("REGION_ID") ENABLE;ALTER TABLE  "DEPARTMENTS" ADD CONSTRAINT "DEPT_LOC_FK" FOREIGN KEY ("LOCATION_ID")
      REFERENCES  "LOCATIONS" ("LOCATION_ID") ENABLE;ALTER TABLE  "DEPARTMENTS" ADD CONSTRAINT "DEPT_MGR_FK" FOREIGN KEY ("MANAGER_ID")
      REFERENCES  "EMPLOYEES" ("EMPLOYEE_ID") ENABLE;ALTER TABLE  "EMPLOYEES" ADD CONSTRAINT "EMP_DEPT_FK" FOREIGN KEY ("DEPARTMENT_ID")
      REFERENCES  "DEPARTMENTS" ("DEPARTMENT_ID") ENABLE;ALTER TABLE  "EMPLOYEES" ADD CONSTRAINT "EMP_JOB_FK" FOREIGN KEY ("JOB_ID")
      REFERENCES  "JOBS" ("JOB_ID") ENABLE;ALTER TABLE  "EMPLOYEES" ADD CONSTRAINT "EMP_MANAGER_FK" FOREIGN KEY ("MANAGER_ID")
      REFERENCES  "EMPLOYEES" ("EMPLOYEE_ID") ENABLE;ALTER TABLE  "JOB_HISTORY" ADD CONSTRAINT "JHIST_DEPT_FK" FOREIGN KEY ("DEPARTMENT_ID")
      REFERENCES  "DEPARTMENTS" ("DEPARTMENT_ID") ENABLE;ALTER TABLE  "JOB_HISTORY" ADD CONSTRAINT "JHIST_EMP_FK" FOREIGN KEY ("EMPLOYEE_ID")
      REFERENCES  "EMPLOYEES" ("EMPLOYEE_ID") ENABLE;ALTER TABLE  "JOB_HISTORY" ADD CONSTRAINT "JHIST_JOB_FK" FOREIGN KEY ("JOB_ID")
      REFERENCES  "JOBS" ("JOB_ID") ENABLE;ALTER TABLE  "LOCATIONS" ADD CONSTRAINT "LOC_C_ID_FK" FOREIGN KEY ("COUNTRY_ID")
      REFERENCES  "COUNTRIES" ("COUNTRY_ID") ENABLE;CREATE OR REPLACE FUNCTION  "EMP_FULLNAME" (EMP_ID IN NUMBER) 
  RETURN VARCHAR2
IS
  retval VARCHAR2(256);
BEGIN
  SELECT (first_name || ' ' || last_name) into retval
  FROM employees
  WHERE employee_id = EMP_ID;
  RETURN retval;
END EMP_FULLNAME;


CREATE OR REPLACE PACKAGE  "TEST_PACKAGE" AS 

  -- TODO enter package declarations (types, exceptions, methods etc) here
  SUBTYPE fullname_t IS VARCHAR2(256);
  
  FUNCTION fullname (
    last_in employees.last_name%TYPE,
    first_in employees.first_name%TYPE)
    RETURN fullname_t;
    
  PROCEDURE INSERT_JOB_HISTORY
    (p_emp_id          job_history.employee_id%type
     , p_start_date      job_history.start_date%type
     , p_end_date        job_history.end_date%type
     , p_job_id          job_history.job_id%type
     , p_department_id   job_history.department_id%type
    );

END TEST_PACKAGE;


CREATE OR REPLACE PACKAGE BODY  "TEST_PACKAGE" AS

  FUNCTION fullname (
    last_in employees.last_name%TYPE,
    first_in employees.first_name%TYPE)
    RETURN fullname_t AS
  BEGIN
    RETURN last_in || ', ' || first_in;
  END fullname;
  
  PROCEDURE insert_job_history
  (  p_emp_id          job_history.employee_id%type
   , p_start_date      job_history.start_date%type
   , p_end_date        job_history.end_date%type
   , p_job_id          job_history.job_id%type
   , p_department_id   job_history.department_id%type
   )
  IS
  BEGIN
    INSERT INTO job_history (employee_id, start_date, end_date,
                             job_id, department_id)
      VALUES(p_emp_id, p_start_date, p_end_date, p_job_id, p_department_id);
  END insert_job_history; 

END TEST_PACKAGE;


CREATE OR REPLACE PROCEDURE  "SECURE_DML" 
IS
BEGIN
  IF TO_CHAR (SYSDATE, 'HH24:MI') NOT BETWEEN '08:00' AND '18:00'
        OR TO_CHAR (SYSDATE, 'DY') IN ('SAT', 'SUN') THEN
    RAISE_APPLICATION_ERROR (-20205,
        'You may only make changes during normal office hours');
  END IF;
END secure_dml;


CREATE OR REPLACE PROCEDURE  "GET_LOCATIONS_AND_REGIONS" (locations OUT SYS_REFCURSOR, regions OUT SYS_REFCURSOR)   
    IS   
    BEGIN   
    OPEN locations FOR SELECT * FROM locations;
    OPEN regions FOR SELECT * FROM regions;
    END "GET_LOCATIONS_AND_REGIONS";


CREATE OR REPLACE PROCEDURE  "GET_EMPLOYEES_STARTING_AFTER" (p_hire_date IN DATE, results OUT SYS_REFCURSOR)   
    IS   
    BEGIN   
    OPEN results FOR SELECT * FROM employees WHERE hire_date >= p_hire_date;
    END "GET_EMPLOYEES_STARTING_AFTER";


CREATE OR REPLACE PROCEDURE  "GET_EMPLOYEES" (catCur OUT SYS_REFCURSOR )   
    IS   
    BEGIN   
    OPEN catCur FOR SELECT * FROM employees;   
    END;


CREATE OR REPLACE PROCEDURE  "ADD_JOB_HISTORY" 
  (  p_emp_id          job_history.employee_id%type
   , p_start_date      job_history.start_date%type
   , p_end_date        job_history.end_date%type
   , p_job_id          job_history.job_id%type
   , p_department_id   job_history.department_id%type
   )
IS
BEGIN
  INSERT INTO job_history (employee_id, start_date, end_date,
                           job_id, department_id)
    VALUES(p_emp_id, p_start_date, p_end_date, p_job_id, p_department_id);
END add_job_history;


 CREATE SEQUENCE   "LOCATIONS_SEQ"  MINVALUE 1 MAXVALUE 9900 INCREMENT BY 100 START WITH 3300 NOCACHE  NOORDER  NOCYCLE ; CREATE SEQUENCE   "EMPLOYEES_SEQ"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 207 NOCACHE  NOORDER  NOCYCLE ; CREATE SEQUENCE   "DEPARTMENTS_SEQ"  MINVALUE 1 MAXVALUE 9990 INCREMENT BY 10 START WITH 280 NOCACHE  NOORDER  NOCYCLE ;CREATE OR REPLACE FORCE VIEW  "EMP_DETAILS_VIEW" ("EMPLOYEE_ID", "JOB_ID", "MANAGER_ID", "DEPARTMENT_ID", "LOCATION_ID", "COUNTRY_ID", "FIRST_NAME", "LAST_NAME", "SALARY", "COMMISSION_PCT", "DEPARTMENT_NAME", "JOB_TITLE", "CITY", "STATE_PROVINCE", "COUNTRY_NAME", "REGION_NAME") AS 
  SELECT
  e.employee_id,
  e.job_id,
  e.manager_id,
  e.department_id,
  d.location_id,
  l.country_id,
  e.first_name,
  e.last_name,
  e.salary,
  e.commission_pct,
  d.department_name,
  j.job_title,
  l.city,
  l.state_province,
  c.country_name,
  r.region_name
FROM
  employees e,
  departments d,
  jobs j,
  locations l,
  countries c,
  regions r
WHERE e.department_id = d.department_id
  AND d.location_id = l.location_id
  AND l.country_id = c.country_id
  AND c.region_id = r.region_id
  AND j.job_id = e.job_id
WITH READ ONLY;?