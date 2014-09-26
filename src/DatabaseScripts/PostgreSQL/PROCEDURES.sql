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
RETURNS void AS $$
BEGIN   
  SELECT * FROM employees;   
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GET_EMPLOYEES_STARTING_AFTER 
(
  IN STARTDATE DATE  
) 
RETURNS void AS $$
BEGIN
      SELECT * FROM employees
      WHERE HIRE_DATE >= STARTDATE;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION GET_LOCATIONS_AND_REGIONS()
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