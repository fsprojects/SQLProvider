connect hr/password;

CREATE OR REPLACE PACKAGE TEST_PACKAGE AS
  -- TODO enter package declarations (types, exceptions, methods etc) here
  SUBTYPE fullname_t IS VARCHAR2(256);

  FUNCTION fullname
  ( last_in  employees.last_name%TYPE
  , first_in employees.first_name%TYPE
  ) RETURN fullname_t;

  PROCEDURE insert_job_history
  ( p_emp_id        job_history.employee_id%type
  , p_start_date    job_history.start_date%type
  , p_end_date      job_history.end_date%type
  , p_job_id        job_history.job_id%type
  , p_department_id job_history.department_id%type
  );
END TEST_PACKAGE;
/

CREATE OR REPLACE PACKAGE BODY TEST_PACKAGE AS
  FUNCTION fullname
  ( last_in  employees.last_name%TYPE
  , first_in employees.first_name%TYPE
  ) RETURN fullname_t IS
  BEGIN
    RETURN last_in || ', ' || first_in;
  END fullname;

  PROCEDURE insert_job_history
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
  END insert_job_history;
END TEST_PACKAGE;
/

COMMIT;
