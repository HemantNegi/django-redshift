CREATE TABLE test_data (
      test_id INT IDENTITY NOT NULL PRIMARY KEY,
      value VARCHAR(25) NOT NULL
);

/*
Multi-line
comment
*/
INSERT INTO test_data (value) VALUES ('Hello');
