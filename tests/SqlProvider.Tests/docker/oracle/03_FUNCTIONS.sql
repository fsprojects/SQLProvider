connect hr/password;

CREATE OR REPLACE FUNCTION EMP_FULLNAME (EMP_ID IN NUMBER)
  RETURN VARCHAR2
IS
  retval VARCHAR2(256);
BEGIN
  SELECT (first_name || ' ' || last_name) into retval
  FROM employees
  WHERE employee_id = EMP_ID;
  RETURN retval;
END EMP_FULLNAME;
/

CREATE OR REPLACE PROCEDURE SECURE_DML
IS
BEGIN
  IF TO_CHAR (SYSDATE, 'HH24:MI') NOT BETWEEN '08:00' AND '18:00'
  OR TO_CHAR (SYSDATE, 'DY') IN ('SAT', 'SUN')
  THEN
    RAISE_APPLICATION_ERROR(
      -20205, 'You may only make changes during normal office hours'
    );
  END IF;
END SECURE_DML;
/

CREATE OR REPLACE PROCEDURE GET_LOCATIONS_AND_REGIONS
( locations OUT SYS_REFCURSOR
, regions OUT SYS_REFCURSOR
) IS
BEGIN
  OPEN locations FOR SELECT * FROM locations;
  OPEN regions FOR SELECT * FROM regions;
END GET_LOCATIONS_AND_REGIONS;
/

CREATE OR REPLACE PROCEDURE GET_EMPLOYEES_STARTING_AFTER
( p_hire_date IN DATE
, results OUT SYS_REFCURSOR
) IS
BEGIN
  OPEN results FOR SELECT * FROM employees WHERE hire_date >= p_hire_date;
END GET_EMPLOYEES_STARTING_AFTER;
/

CREATE OR REPLACE PROCEDURE GET_EMPLOYEES
( catCur OUT SYS_REFCURSOR
) IS
BEGIN
  OPEN catCur FOR SELECT * FROM employees;
END GET_EMPLOYEES;
/

CREATE OR REPLACE PROCEDURE ADD_JOB_HISTORY
( p_emp_id        job_history.employee_id%type
, p_start_date    job_history.start_date%type
, p_end_date      job_history.end_date%type
, p_job_id        job_history.job_id%type
, p_department_id job_history.department_id%type
) IS
BEGIN
  INSERT INTO job_history
  ( employee_id
  , start_date
  , end_date
  , job_id
  , department_id
  )
  VALUES
  ( p_emp_id
  , p_start_date
  , p_end_date
  , p_job_id
  , p_department_id
  );
END ADD_JOB_HISTORY;
/

CREATE OR REPLACE PROCEDURE CLEAR_JOB_HISTORY
( p_emp_id job_history.employee_id%type
) IS
BEGIN
  DELETE FROM job_history WHERE employee_id = p_emp_id;
END CLEAR_JOB_HISTORY;
/

COMMIT;
