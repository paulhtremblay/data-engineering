WITH rn_next AS (
  SELECT
    id,
    start_time,
    end_time,
    LAG(end_time, 1) over(partition by id order by start_time) as previous_end_time,
    lead(start_time,1) over (partition by id order by start_time) as next_start_time,
    row_number() over(partition by id order by start_time) as rn,
    row_number() over(partition by id order by start_time desc) as rn_r,
  FROM `paul-henry-tremblay.data_engineering.tv_streaming`
),
breakpoints as (
  SELECT *,
    CASE
      WHEN DATETIME_DIFF(start_time, previous_end_time,  SECOND) IS NULL
           OR DATETIME_DIFF(start_time, previous_end_time,  SECOND) != 0 THEN true
      WHEN rn = 1 then true
      ELSE false
    END AS start,
    CASE
      WHEN DATETIME_DIFF(next_start_time, end_time,  SECOND) IS NULL
        OR  DATETIME_DIFF(next_start_time, end_time,  SECOND) != 0 THEN true
      WHEN rn_r = 1 then true
      ELSE false
    END AS end_
  FROM rn_next
),start_numbers as (
  SELECT id, start_time, end_time,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_time) as start_rn,
  FROM breakpoints
  WHERE start
), end_numbers AS (
  SELECT id, start_time, end_time,
      ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_time) AS end_rn,
  FROM breakpoints
  WHERE end_
),
lookup_t as (
  SELECT sn.id,
  sn.start_time,
  en.end_time,
  sn.start_rn as gn
  from start_numbers sn
  INNER JOIN end_numbers en
  ON sn.start_rn = en.end_rn
  and sn.id = en.id
), grouped AS (
  SELECT s.*, l.gn, l.start_time as group_start_time, l.end_time as group_end_time
  FROM `paul-henry-tremblay.data_engineering.tv_streaming` s
  INNER JOIN lookup_t l
  ON l.id = s.id
  AND s.start_time >= l.start_time
  AND s.end_time <= l.end_time
), as_array as (
  SELECT id, group_start_time AS start_time, group_end_time AS end_time,
  ARRAY_AGG(STRUCT( start_time AS start_time, end_time AS end_time, bytes as bytes, show_name AS show_name)) AS streaming
  FROM grouped
  GROUP BY id, group_start_time, group_end_time

)
SELECT *
FROM grouped
ORDER BY id, start_time
