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
