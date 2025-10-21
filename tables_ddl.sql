-- EXTERNAL TABLES

CREATE EXTERNAL TABLE `wp_content.articles_extract`(
  `link` string, 
  `title` string, 
  `content` string, 
  `user` string, 
  `date` string, 
  `tags` array<string>, 
  `user_flag` boolean, 
  `authors` array<string>, 
  `edition` string,
  `reading_time` integer)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://guiaeciencia-bucket/data/wp_content/articles_extract/'
;


-- VIEWS

CREATE OR REPLACE VIEW wp_content.post_author AS 
SELECT
  author,
  date,
  title,
  edition
FROM
  (wp_content.articles_extract
CROSS JOIN UNNEST(authors) t (author))
;

CREATE OR REPLACE VIEW wp_content.count_release AS 
SELECT
  author,
  count(*) published
FROM
  wp_content.post_author
GROUP BY author
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW wp_content.post_tag AS 
SELECT
  tag,
  date,
  title,
  edition
FROM
  (wp_content.articles_extract
CROSS JOIN UNNEST(tags) t (tag))
;

CREATE OR REPLACE VIEW wp_content.count_tag_use AS 
SELECT
  tag,
  count(*) count_posts
FROM
  wp_content.post_tag
GROUP BY tag
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW wp_content.author_last_release AS 
WITH
 ranked AS (
   SELECT
     author,
     date,
     title,
     edition,
     ROW_NUMBER() OVER (PARTITION BY author ORDER BY date DESC) rn
   FROM
     wp_content.post_author
) 
SELECT
  author,
  date last_release_date,
  title last_release_title,
  edition
FROM
  ranked
WHERE (rn = 1)
ORDER BY last_release_date DESC
;

CREATE OR REPLACE VIEW ga4_content.events_view AS 
SELECT
  t.*,
  regexp_extract(page_title, '\(V\.\d+, N\.\d+, P\.\d+, \d{4}\)$', 0) edition
FROM
  (
   SELECT
     event_date,
     event_name,
     user_pseudo_id,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_title') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_title,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_location') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_location,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_referrer') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_referrer,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'ga_session_id') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_session_id,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'ga_session_number') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_session_number,
     MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'engagement_time_msec') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_engagement_msec,
     extract_date
   FROM
     (ga4_content.ga4_events
   CROSS JOIN UNNEST(CAST(json_parse(event_params) AS array(json))) t (ep))
   GROUP BY event_date, event_name, user_pseudo_id, extract_date
)  t
;

CREATE OR REPLACE VIEW ga4_content.page_referrer_count AS 
SELECT
  edition,
  page_referrer,
  count(*) quantity,
  extract_date
FROM
  ga4_content.events_view
WHERE event_name = 'page_view' AND edition IS NOT NULL
GROUP BY edition, page_referrer, extract_date
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW ga4_content.page_engagement_msec AS 
SELECT
  edition,
  ga_engagement_msec,
  extract_date
FROM
  ga4_content.events_view
WHERE event_name = 'user_engagement' AND edition IS NOT NULL AND ga_engagement_msec IS NOT NULL
;