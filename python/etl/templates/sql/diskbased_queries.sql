SELECT sqs.query
     , trim(left(sq.querytxt, 50)) AS querytxt
     , sqs.seg
     , sqs.step
     , sqs.label
     , sq.starttime
     , ROUND(sqs.maxtime::FLOAT / sqs.avgtime, 3) AS max_avg_ratio
     , sqs.rows
FROM svl_query_summary AS sqs
JOIN stl_query AS sq ON sqs.query = sq.query AND sqs.userid = sq.userid
WHERE sqs.userid = current_user_id
  AND sqs.is_diskbased = 't'
  AND sq.starttime > '${date.yesterday}'
ORDER BY sq.starttime
