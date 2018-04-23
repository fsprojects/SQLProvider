--------------------------------------------------------
--  DDL for Procedure ADD_JOB_HISTORY
--------------------------------------------------------
CREATE PROCEDURE ADD_JOB_HISTORY
(  @p_emp_id          INT
 , @p_start_date      DATE
 , @p_end_date        DATE
 , @p_job_id          VARCHAR(10)
 , @p_department_id   INT
)
AS
BEGIN
  INSERT INTO job_history (employee_id, start_date, end_date, job_id, department_id) VALUES(@p_emp_id, @p_start_date, @p_end_date, @p_job_id, @p_department_id);
END
GO
--------------------------------------------------------
--  DDL for Procedure GET_EMPLOYEES
--------------------------------------------------------
CREATE PROCEDURE GET_EMPLOYEES
AS   
BEGIN   
  SELECT * FROM employees;   
END
GO
--------------------------------------------------------
--  DDL for Procedure GET_EMPLOYEES_STARTING_AFTER
--------------------------------------------------------
CREATE PROCEDURE GET_EMPLOYEES_STARTING_AFTER 
(
  @STARTDATE DATE  
) AS 
BEGIN
      SELECT * FROM employees
      WHERE HIRE_DATE >= @STARTDATE;
END
GO

--------------------------------------------------------
--  DDL for Procedure GET_LOCATIONS_AND_REGIONS
--------------------------------------------------------
CREATE PROCEDURE GET_LOCATIONS_AND_REGIONS 
AS  
BEGIN   
    SELECT * FROM locations;  
    SELECT * FROM regions; 
END

