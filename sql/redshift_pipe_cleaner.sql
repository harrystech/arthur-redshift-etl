-- Create a table with various types to test the write to JSON in Spark followed by COPY in Redshift
CREATE TABLE redshift_pipe_cleaner (
  an_int INTEGER,
  a_default INTEGER DEFAULT 10 NOT NULL,
  a_double DOUBLE PRECISION,
  a_numeric DECIMAL(20,4),
  a_boolean BOOLEAN,
  a_text VARCHAR(255),
  a_date DATE,
  a_timestamp TIMESTAMP
);

INSERT INTO redshift_pipe_cleaner VALUES
  (1, 2, 3.3, 4.4, TRUE, 'Hello', '2016-06-13', '2016-06-13 11:44:00'),
  (101, 202, 303.3, 404.4, FALSE, '', '2016-06-14', '2016-06-14 12:34:56')
;

-- Test some string values that require quoting

INSERT INTO redshift_pipe_cleaner(a_text) VALUES
  (NULL),
  ('"'),
  (','),
  ('Jeffrey "The Dude" Lebowski, a Los Angeles slacker')
;
