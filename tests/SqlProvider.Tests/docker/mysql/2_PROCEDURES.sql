USE HR;
delimiter //
CREATE PROCEDURE ADD_JOB_HISTORY
(  IN p_emp_id          INT
 , IN p_start_date      DATE
 , IN p_end_date        DATE
 , IN p_job_id          VARCHAR(10)
 , IN p_department_id   INT
)
BEGIN
  INSERT INTO job_history (employee_id, start_date, end_date, job_id, department_id) VALUES(p_emp_id, p_start_date, p_end_date, p_job_id, p_department_id);
END//

CREATE PROCEDURE GET_EMPLOYEES() 
BEGIN   
  SELECT * FROM employees;   
END//

CREATE PROCEDURE GET_EMPLOYEES_STARTING_AFTER 
(
  IN STARTDATE DATE  
) 
BEGIN
      SELECT * FROM employees
      WHERE HIRE_DATE >= STARTDATE;
END//

CREATE PROCEDURE GET_LOCATIONS_AND_REGIONS()
BEGIN   
    SELECT * FROM locations;  
    SELECT * FROM regions; 
END//

