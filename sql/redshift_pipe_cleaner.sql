-- Create a table with various types to test the write to JSON in Spark followed by COPY in Redshift
CREATE TABLE IF NOT EXISTS redshift_pipe_cleaner (
  id SERIAL, -- to more easily sort and compare values
  an_int INTEGER,
  a_double DOUBLE PRECISION,
  a_numeric DECIMAL(20,4),
  a_boolean BOOLEAN,
  a_text VARCHAR(255),
  a_date DATE,
  a_timestamp TIMESTAMP
);

TRUNCATE TABLE redshift_pipe_cleaner;

-- Test some basic values or all basic data types

INSERT INTO redshift_pipe_cleaner(an_int, a_double, a_numeric, a_boolean, a_text, a_date, a_timestamp) VALUES
  (1, 3.3, 4.1234, TRUE, 'Hello', '2016-06-13', '2016-06-13 11:44:00'),
  (101, 303.3, 404.5678, FALSE, '"World"', '2016-06-14', '2016-06-14 12:34:56')
;

-- Test some string values that require quoting

INSERT INTO redshift_pipe_cleaner(a_text) VALUES
  (NULL),
  (''),  -- test empty string is not null
  ('"Start quote'),
  ('End quote"'),
  ('A comma -> , <- within the line'),
  ('Jeffrey "The Dude" Lebowski, a Los Angeles slacker'),
  ('''This'' line\nbroke.'),
  ('And now throwing it all in:\n''single'' and "double" quotes with line\nbreaks and commas.')
;
