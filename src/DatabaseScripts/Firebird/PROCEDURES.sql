set term ^ ; 
CREATE PROCEDURE ADD_JOB_HISTORY
(  p_emp_id          INT
 , p_start_date      DATE
 , p_end_date        DATE
 , p_job_id          VARCHAR(10)
 , p_department_id   INT
)
AS
BEGIN
  INSERT INTO JOB_HISTORY (employee_id, start_date, end_date, job_id, department_id) VALUES(:p_emp_id, :p_start_date, :p_end_date, :p_job_id, :p_department_id);
END^

CREATE PROCEDURE GET_EMPLOYEES
RETURNS ( 
  EMPLOYEE_ID INTEGER, 
  FIRST_NAME VARCHAR(20), 
  LAST_NAME VARCHAR(25), 
  EMAIL VARCHAR(25), 
  PHONE_NUMBER VARCHAR(20), 
  HIRE_DATE DATE, 
  JOB_ID VARCHAR(10), 
  SALARY INTEGER, 
  COMMISSION_PCT INTEGER, 
  MANAGER_ID INTEGER, 
  DEPARTMENT_ID INTEGER
)
AS
BEGIN
  FOR SELECT 
    a.EMPLOYEE_ID, 
    a.FIRST_NAME, 
    a.LAST_NAME, 
    a.EMAIL, 
    a.PHONE_NUMBER, 
    a.HIRE_DATE, 
    a.JOB_ID, 
    a.SALARY, 
    a.COMMISSION_PCT, 
    a.MANAGER_ID, 
    a.DEPARTMENT_ID
  FROM EMPLOYEES a
  INTO 
    EMPLOYEE_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    PHONE_NUMBER, 
    HIRE_DATE, 
    JOB_ID, 
    SALARY, 
    COMMISSION_PCT, 
    MANAGER_ID, 
    DEPARTMENT_ID 
  DO
  BEGIN
    SUSPEND;
  END
END^

CREATE PROCEDURE GET_EMPLOYEES_STARTING_AFTER 
(
  STARTDATE DATE  
) 
RETURNS ( 
  EMPLOYEE_ID INTEGER, 
  FIRST_NAME VARCHAR(20), 
  LAST_NAME VARCHAR(25), 
  EMAIL VARCHAR(25), 
  PHONE_NUMBER VARCHAR(20), 
  HIRE_DATE DATE, 
  JOB_ID VARCHAR(10), 
  SALARY INTEGER, 
  COMMISSION_PCT INTEGER, 
  MANAGER_ID INTEGER, 
  DEPARTMENT_ID INTEGER
)
AS
BEGIN
  FOR SELECT 
    a.EMPLOYEE_ID, 
    a.FIRST_NAME, 
    a.LAST_NAME, 
    a.EMAIL, 
    a.PHONE_NUMBER, 
    a.HIRE_DATE, 
    a.JOB_ID, 
    a.SALARY, 
    a.COMMISSION_PCT, 
    a.MANAGER_ID, 
    a.DEPARTMENT_ID
  FROM EMPLOYEES a
      WHERE HIRE_DATE >= :STARTDATE
  INTO 
    EMPLOYEE_ID, 
    FIRST_NAME, 
    LAST_NAME, 
    EMAIL, 
    PHONE_NUMBER, 
    HIRE_DATE, 
    JOB_ID, 
    SALARY, 
    COMMISSION_PCT, 
    MANAGER_ID, 
    DEPARTMENT_ID do suspend;
END^

/* This test in Firebird is useless, because we would need 
to put in a Returning clause some fields and convert the fields 
of the two tables to some common format

CREATE PROCEDURE GET_LOCATIONS_AND_REGIONS()
BEGIN   
    SELECT * FROM locations;  
    SELECT * FROM regions; 
END^
*/

set term ; ^