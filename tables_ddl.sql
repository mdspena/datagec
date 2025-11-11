-- EXTERNAL TABLES

CREATE EXTERNAL TABLE `books`(
  `book` string COMMENT 'from deserializer', 
  `publisher_or_provider` string COMMENT 'from deserializer', 
  `post` string COMMENT 'from deserializer', 
  `edition` string COMMENT 'from deserializer', 
  `usage_request_date` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'escapeChar'='\\', 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket-name/data/books'
TBLPROPERTIES (
  'classification'='csv', 
  'has_encrypted_data'='false', 
  'projection.enabled'='false', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1762151855')
;

CREATE EXTERNAL TABLE `historic_views`(
  `edition` string COMMENT 'from deserializer', 
  `201903` string COMMENT 'from deserializer', 
  `201904` string COMMENT 'from deserializer', 
  `201905` string COMMENT 'from deserializer', 
  `201906` string COMMENT 'from deserializer', 
  `201907` string COMMENT 'from deserializer', 
  `201908` string COMMENT 'from deserializer', 
  `201909` string COMMENT 'from deserializer', 
  `201910` string COMMENT 'from deserializer', 
  `201911` string COMMENT 'from deserializer', 
  `201912` string COMMENT 'from deserializer', 
  `202001` string COMMENT 'from deserializer', 
  `202002` string COMMENT 'from deserializer', 
  `202003` string COMMENT 'from deserializer', 
  `202004` string COMMENT 'from deserializer', 
  `202005` string COMMENT 'from deserializer', 
  `202006` string COMMENT 'from deserializer', 
  `202007` string COMMENT 'from deserializer', 
  `202008` string COMMENT 'from deserializer', 
  `202009` string COMMENT 'from deserializer', 
  `202010` string COMMENT 'from deserializer', 
  `202011` string COMMENT 'from deserializer', 
  `202012` string COMMENT 'from deserializer', 
  `202101` string COMMENT 'from deserializer', 
  `202102` string COMMENT 'from deserializer', 
  `202103` string COMMENT 'from deserializer', 
  `202104` string COMMENT 'from deserializer', 
  `202105` string COMMENT 'from deserializer', 
  `202106` string COMMENT 'from deserializer', 
  `202107` string COMMENT 'from deserializer', 
  `202108` string COMMENT 'from deserializer', 
  `202109` string COMMENT 'from deserializer', 
  `202110` string COMMENT 'from deserializer', 
  `202111` string COMMENT 'from deserializer', 
  `202112` string COMMENT 'from deserializer', 
  `202201` string COMMENT 'from deserializer', 
  `202202` string COMMENT 'from deserializer', 
  `202203` string COMMENT 'from deserializer', 
  `202204` string COMMENT 'from deserializer', 
  `202205` string COMMENT 'from deserializer', 
  `202206` string COMMENT 'from deserializer', 
  `202207` string COMMENT 'from deserializer', 
  `202208` string COMMENT 'from deserializer', 
  `202209` string COMMENT 'from deserializer', 
  `202210` string COMMENT 'from deserializer', 
  `202211` string COMMENT 'from deserializer', 
  `202212` string COMMENT 'from deserializer', 
  `202301` string COMMENT 'from deserializer', 
  `202302` string COMMENT 'from deserializer', 
  `202303` string COMMENT 'from deserializer', 
  `202304` string COMMENT 'from deserializer', 
  `202305` string COMMENT 'from deserializer', 
  `202306` string COMMENT 'from deserializer', 
  `202307` string COMMENT 'from deserializer', 
  `202308` string COMMENT 'from deserializer', 
  `202309` string COMMENT 'from deserializer', 
  `202310` string COMMENT 'from deserializer', 
  `202311` string COMMENT 'from deserializer', 
  `202312` string COMMENT 'from deserializer', 
  `202401` string COMMENT 'from deserializer', 
  `202402` string COMMENT 'from deserializer', 
  `total` string COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES ( 
  'escapeChar'='\\', 
  'quoteChar'='\"', 
  'separatorChar'=',') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bucket-name/data/historic_views'
TBLPROPERTIES (
  'classification'='csv', 
  'has_encrypted_data'='false', 
  'skip.header.line.count'='1', 
  'transient_lastDdlTime'='1762156701')
;

CREATE EXTERNAL TABLE `articles_extract`(
  `link` string, 
  `title` string, 
  `content` string, 
  `user` string, 
  `date` string, 
  `tags` array<string>, 
  `user_flag` boolean, 
  `authors` array<string>, 
  `edition` string, 
  `reading_time` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bucket-name/data/wp_content/articles_extract'
TBLPROPERTIES (
  'transient_lastDdlTime'='1761013267')
;

CREATE EXTERNAL TABLE `ga4_events`(
  `event_date` string, 
  `event_timestamp` string, 
  `event_name` string, 
  `event_params` string, 
  `event_previous_timestamp` string, 
  `event_value_in_usd` string, 
  `event_bundle_sequence_id` string, 
  `event_server_timestamp_offset` string, 
  `user_id` string, 
  `user_pseudo_id` string, 
  `privacy_info` string, 
  `user_properties` string, 
  `user_first_touch_timestamp` string, 
  `user_ltv` string, 
  `device` string, 
  `geo` string, 
  `app_info` string, 
  `traffic_source` string, 
  `stream_id` string, 
  `platform` string, 
  `event_dimensions` string, 
  `ecommerce` string, 
  `items` string, 
  `collected_traffic_source` string, 
  `is_active_user` string, 
  `batch_event_index` string, 
  `batch_page_id` string, 
  `batch_ordering_id` string, 
  `session_traffic_source_last_click` string, 
  `publisher` string)
PARTITIONED BY ( 
  `extract_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://bucket-name/ga4_content/ga4_events'
TBLPROPERTIES (
  'classification'='parquet', 
  'compressionType'='snappy', 
  'projection.enabled'='false', 
  'typeOfData'='file')
;

-- VIEWS

CREATE OR REPLACE VIEW "post_metadata" AS 
WITH
  books_count AS (
   SELECT
     edition
   , count(*) books_usage
   FROM
     archive.books
   GROUP BY edition
) 
, total_views_new AS (
   SELECT
     edition
   , sum(views) views
   FROM
     ga4_content.views_per_month
   GROUP BY edition
) 
SELECT
  edition "Edição"
, date(from_iso8601_timestamp(ae.date)) "Data de publicação"
, replace(replace(replace(replace(ae.title, '&#8211;', U&'\2014'), '&#8230;', '...'), '&#8220;', ''), '&#8221;', '') "Post"
, CAST(bc.books_usage AS int) "Uso em livros didáticos"
, CAST(epp.engagement_seconds AS int) "Tempo médio de visualização"
, CAST(ir.institutional_ref AS int) "Origem de tráfego institucional"
, CAST(sr.social_ref AS int) "Origem de tráfego social"
, CAST((COALESCE(tv.views, 0) + COALESCE(hv.total, 0)) AS int) "Total de visualizações"
FROM
  ((((((wp_content.articles_extract ae
LEFT JOIN ga4_content.engagement_per_post epp USING (edition))
LEFT JOIN books_count bc USING (edition))
LEFT JOIN ga4_content.social_ref sr USING (edition))
LEFT JOIN ga4_content.institutional_ref ir USING (edition))
LEFT JOIN total_views_new tv USING (edition))
LEFT JOIN archive.historic_views hv USING (edition))
ORDER BY ae.date ASC
;

CREATE OR REPLACE VIEW "vw_views_per_month" AS 
WITH
  historic AS (
   SELECT
     edition
   , "201903" m201903
   , "201904" m201904
   , "201905" m201905
   , "201906" m201906
   , "201907" m201907
   , "201908" m201908
   , "201909" m201909
   , "201910" m201910
   , "201911" m201911
   , "201912" m201912
   , "202001" m202001
   , "202002" m202002
   , "202003" m202003
   , "202004" m202004
   , "202005" m202005
   , "202006" m202006
   , "202007" m202007
   , "202008" m202008
   , "202009" m202009
   , "202010" m202010
   , "202011" m202011
   , "202012" m202012
   , "202101" m202101
   , "202102" m202102
   , "202103" m202103
   , "202104" m202104
   , "202105" m202105
   , "202106" m202106
   , "202107" m202107
   , "202108" m202108
   , "202109" m202109
   , "202110" m202110
   , "202111" m202111
   , "202112" m202112
   , "202201" m202201
   , "202202" m202202
   , "202203" m202203
   , "202204" m202204
   , "202205" m202205
   , "202206" m202206
   , "202207" m202207
   , "202208" m202208
   , "202209" m202209
   , "202210" m202210
   , "202211" m202211
   , "202212" m202212
   , "202301" m202301
   , "202302" m202302
   , "202303" m202303
   , "202304" m202304
   , "202305" m202305
   , "202306" m202306
   , "202307" m202307
   , "202308" m202308
   , "202309" m202309
   , "202310" m202310
   , "202311" m202311
   , "202312" m202312
   , "202401" m202401
   , "202402" m202402
   FROM
     archive.historic_views
) 
, ga4 AS (
   SELECT
     edition
   , COALESCE(MAX((CASE WHEN (month = '202403') THEN views END)), 0) m202403
   , COALESCE(MAX((CASE WHEN (month = '202404') THEN views END)), 0) m202404
   , COALESCE(MAX((CASE WHEN (month = '202405') THEN views END)), 0) m202405
   , COALESCE(MAX((CASE WHEN (month = '202406') THEN views END)), 0) m202406
   , COALESCE(MAX((CASE WHEN (month = '202407') THEN views END)), 0) m202407
   , COALESCE(MAX((CASE WHEN (month = '202408') THEN views END)), 0) m202408
   , COALESCE(MAX((CASE WHEN (month = '202409') THEN views END)), 0) m202409
   , COALESCE(MAX((CASE WHEN (month = '202410') THEN views END)), 0) m202410
   , COALESCE(MAX((CASE WHEN (month = '202411') THEN views END)), 0) m202411
   , COALESCE(MAX((CASE WHEN (month = '202412') THEN views END)), 0) m202412
   , COALESCE(MAX((CASE WHEN (month = '202501') THEN views END)), 0) m202501
   , COALESCE(MAX((CASE WHEN (month = '202502') THEN views END)), 0) m202502
   , COALESCE(MAX((CASE WHEN (month = '202503') THEN views END)), 0) m202503
   , COALESCE(MAX((CASE WHEN (month = '202504') THEN views END)), 0) m202504
   , COALESCE(MAX((CASE WHEN (month = '202505') THEN views END)), 0) m202505
   , COALESCE(MAX((CASE WHEN (month = '202506') THEN views END)), 0) m202506
   , COALESCE(MAX((CASE WHEN (month = '202507') THEN views END)), 0) m202507
   , COALESCE(MAX((CASE WHEN (month = '202508') THEN views END)), 0) m202508
   , COALESCE(MAX((CASE WHEN (month = '202509') THEN views END)), 0) m202509
   , COALESCE(MAX((CASE WHEN (month = '202510') THEN views END)), 0) m202510
   , COALESCE(MAX((CASE WHEN (month = '202511') THEN views END)), 0) m202511
   , COALESCE(MAX((CASE WHEN (month = '202512') THEN views END)), 0) m202512
   FROM
     ga4_content.views_per_month
   GROUP BY edition
   ORDER BY edition ASC
) 
SELECT *
FROM
  (historic
INNER JOIN ga4 USING (edition))
;

CREATE OR REPLACE VIEW "author_last_release" AS 
WITH
  ranked AS (
   SELECT
     author
   , date
   , title
   , edition
   , ROW_NUMBER() OVER (PARTITION BY author ORDER BY date DESC) rn
   FROM
     wp_content.post_author
) 
SELECT
  author
, date last_release_date
, title last_release_title
, edition
FROM
  ranked
WHERE (rn = 1)
ORDER BY last_release_date DESC
;

CREATE OR REPLACE VIEW "count_release" AS 
SELECT
  author
, count(*) published
FROM
  wp_content.post_author
GROUP BY author
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW "count_tag_use" AS 
SELECT
  tag
, count(*) count_posts
FROM
  wp_content.post_tag
GROUP BY tag
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW "post_author" AS 
SELECT
  author
, date
, title
, edition
FROM
  (wp_content.articles_extract
CROSS JOIN UNNEST(authors) t (author))
;

CREATE OR REPLACE VIEW "post_tag" AS 
SELECT
  tag
, date
, title
, edition
FROM
  (wp_content.articles_extract
CROSS JOIN UNNEST(tags) t (tag))
;

CREATE OR REPLACE VIEW "title_edition" AS 
(
   SELECT
     title
   , edition
   FROM
     wp_content.articles_extract
) 
;

CREATE OR REPLACE VIEW "engagement_per_post" AS 
(
   SELECT
     edition
   , CAST((avg(CAST(ga_engagement_msec AS int)) / 1000) AS int) engagement_seconds
   FROM
     ga4_content.page_engagement_msec
   GROUP BY edition
) 
;

CREATE OR REPLACE VIEW "events_view" AS 
SELECT
  t.*
, regexp_extract(page_title, '\(V\.\d+, N\.\d+, P\.\d+, \d{4}\)$', 0) edition
FROM
  (
   SELECT
     event_date
   , event_name
   , user_pseudo_id
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_title') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_title
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_location') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_location
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'page_referrer') THEN json_extract_scalar(ep, '$.value.string_value') END)) page_referrer
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'ga_session_id') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_session_id
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'ga_session_number') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_session_number
   , MAX((CASE WHEN (json_extract_scalar(ep, '$.key') = 'engagement_time_msec') THEN json_extract_scalar(ep, '$.value.int_value') END)) ga_engagement_msec
   , extract_date
   FROM
     (ga4_content.ga4_events
   CROSS JOIN UNNEST(CAST(json_parse(event_params) AS array(json))) t (ep))
   GROUP BY event_date, event_name, user_pseudo_id, extract_date
)  t
;

CREATE OR REPLACE VIEW "institutional_ref" AS 
(
   SELECT
     edition
   , count(*) institutional_ref
   FROM
     (ga4_content.page_referrer_count
   INNER JOIN wp_content.title_edition USING (edition))
   WHERE (((page_referrer LIKE '%.edu.%') OR (page_referrer LIKE '%scholar.%') OR (page_referrer LIKE '%moodle.%') OR (page_referrer LIKE '%classroom.%') OR (page_referrer LIKE '%gov.%') OR (page_referrer LIKE '%cnpq.%')) AND (NOT (page_referrer LIKE '%www.google.com.br/url%')) AND (NOT (page_referrer LIKE '%www.google.com/url%')) AND (NOT (page_referrer LIKE '%search.yahoo%')) AND (NOT (page_referrer LIKE '%/gec.%')))
   GROUP BY edition
) 
;

CREATE OR REPLACE VIEW "social_ref" AS 
(
   SELECT
     edition
   , count(*) social_ref
   FROM
     ga4_content.page_referrer_count
   WHERE ((page_referrer LIKE '%t.co/%') OR (page_referrer LIKE '%instagram.%') OR (page_referrer LIKE '%spotify.%') OR (page_referrer LIKE '%facebook.%') OR (page_referrer LIKE '%bsky.%') OR (page_referrer LIKE '%youtube.%'))
   GROUP BY edition
) 
;

CREATE OR REPLACE VIEW "page_engagement_msec" AS 
SELECT
  edition
, ga_engagement_msec
, extract_date
FROM
  ga4_content.events_view
WHERE ((event_name = 'user_engagement') AND (edition IS NOT NULL) AND (ga_engagement_msec IS NOT NULL))
;

CREATE OR REPLACE VIEW "page_referrer_count" AS 
SELECT
  edition
, page_referrer
, count(*) quantity
, extract_date
FROM
  ga4_content.events_view
WHERE ((event_name = 'page_view') AND (edition IS NOT NULL))
GROUP BY edition, page_referrer, extract_date
ORDER BY 2 DESC
;

CREATE OR REPLACE VIEW "views_per_month" AS 
SELECT
  substring(extract_date, 1, 6) month
, edition
, count(*) views
FROM
  "ga4_content"."events_view"
WHERE ((event_name = 'page_view') AND (edition IS NOT NULL))
GROUP BY edition, substring(extract_date, 1, 6)
ORDER BY 1 ASC
;