\c travis_ci_test;
create table test_table (
  id    SERIAL      PRIMARY KEY,
  ss    VARCHAR(20) NULL,
  dd    INT         NULL,
  dt    TIMESTAMP   NULL
);
