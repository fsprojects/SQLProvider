USE HR;

DELIMITER $$
CREATE FUNCTION EMP_FULLNAME (EMP_ID int) 
RETURNS VARCHAR(50)
BEGIN
  DECLARE retVal varchar(50);
  SELECT concat_ws(' ', first_name, last_name) into retval
  FROM employees
  WHERE employee_id = EMP_ID;
  RETURN retval;
END$$;
