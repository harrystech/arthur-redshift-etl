-- Collect time spent on compiling queries within last 24 hours
SELECT TRIM(u.usename) AS user_name
     , CASE c.locus
           WHEN 1 THEN 'compute'
           WHEN 2 THEN 'leader'
       END AS locus_name
     , COUNT(DISTINCT c.query) AS total_queries
     , SUM(DATEDIFF(MS, c.starttime, c.endtime)) AS total_compile_time_ms
FROM svl_compile AS c
JOIN pg_user AS u ON c.userid = u.usesysid
WHERE c.compile = 1 AND DATEADD(DAY, -1, SYSDATE) < c.starttime
GROUP BY u.usename, locus_name
ORDER BY u.usename, locus_name
