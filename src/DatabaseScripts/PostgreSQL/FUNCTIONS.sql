CREATE OR REPLACE FUNCTION EMP_FULLNAME (EMP_ID int)
RETURNS text AS $retval$
DECLARE
    retval text;
BEGIN
    SELECT CONCAT_WS(' ',employees.first_name,employees.last_name) into retval
    FROM public.employees WHERE employees.employee_id = EMP_ID;
    RETURN retval;
END;
$retval$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION ADD_JOB_HISTORY
(  IN p_emp_id          INT
 , IN p_start_date      DATE
 , IN p_end_date        DATE
 , IN p_job_id          VARCHAR(10)
 , IN p_department_id   INT
)
RETURNS void AS $$
BEGIN
    INSERT INTO job_history (employee_id, start_date, end_date, job_id, department_id) VALUES(p_emp_id, p_start_date, p_end_date, p_job_id, p_department_id);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GET_EMPLOYEES() 
RETURNS refcursor AS $$
DECLARE
    employees_out refcursor;
BEGIN
    OPEN employees_out FOR
    SELECT * FROM employees;
    RETURN employees_out;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GET_DEPARTMENTS()
RETURNS TABLE (
    department_id INT,
    department_name VARCHAR(30)
) AS $$
BEGIN
    RETURN query
    SELECT departments.department_id, departments.department_name FROM departments;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION GET_EMPLOYEES_STARTING_AFTER (IN STARTDATE DATE)
RETURNS refcursor AS $$
DECLARE
    employees_out refcursor;
BEGIN
    OPEN employees_out FOR
    SELECT * FROM employees
    WHERE HIRE_DATE >= STARTDATE;
    RETURN employees_out;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GET_LOCATIONS_AND_REGIONS ()
RETURNS SETOF refcursor AS $$
DECLARE
    region_out refcursor;
    location_out refcursor;
BEGIN   
    OPEN location_out FOR
    SELECT * FROM locations;
    RETURN NEXT location_out;

    OPEN region_out FOR
    SELECT * FROM regions;
    RETURN NEXT region_out;

    RETURN;
END;
$$ LANGUAGE plpgsql;
