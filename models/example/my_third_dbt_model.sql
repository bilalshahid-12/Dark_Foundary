WITH source AS (
    SELECT *
    FROM {{ source('dbt_bilalshahid12','staging_table') }}
    -- FROM 'dark-foundry-340620.dbt_bilalshahid12.staging_table'
)

SELECT 
    jobs_results,
    chips,
    search_metadata_id,
    search_metadata_status,
    search_metadata_json_endpoint,
    search_metadata_created_at,
    search_metadata_processed_at,
    search_metadata_google_jobs_url,
    search_metadata_raw_html_file,
    search_metadata_total_time_taken,
    search_parameters_q,
    search_parameters_engine,
    search_parameters_google_domain,
    title,
    company_name,
    location,
    via,
    description,
    job_highlights,
    JSON_EXTRACT_SCALAR(job_highlights, '$[0].title') AS title_1,
    JSON_EXTRACT_SCALAR(job_highlights, '$[1].title') AS title_2,
    JSON_EXTRACT_SCALAR(job_highlights, '$[2].title') AS title_3,
    JSON_EXTRACT(job_highlights, '$[0].items') AS items,
    JSON_EXTRACT(job_highlights, '$[1].items') AS items_1,
    JSON_EXTRACT(job_highlights, '$[2].items') AS items_2,
    related_links,
    JSON_EXTRACT(related_links, '$[0].link') AS link,
    JSON_EXTRACT(related_links, '$[0].text') AS text_1,
    JSON_EXTRACT(related_links, '$[1].link') AS link_1,
    JSON_EXTRACT(related_links, '$[1].text') AS text_2,
    type,
    param,
    text

FROM
(
SELECT
    source.*,
    JSON_EXTRACT_SCALAR(source.jobs_results, '$[0].title') AS title,
    JSON_EXTRACT_SCALAR(source.jobs_results, '$[0].company_name') AS company_name,
    JSON_EXTRACT_SCALAR(source.jobs_results, '$[0].location') AS location,
    JSON_EXTRACT_SCALAR(source.jobs_results, '$[0].via') AS via,
    JSON_EXTRACT_SCALAR(source.jobs_results, '$[0].description') AS description,
    JSON_EXTRACT( jobs_results, '$[0].job_highlights' ) AS job_highlights,
    JSON_EXTRACT( jobs_results, '$[0].related_links' ) AS related_links,
    JSON_EXTRACT_SCALAR(chips, '$[0].type') AS type,
    JSON_EXTRACT_SCALAR(chips, '$[0].param') AS param,
    JSON_EXTRACT_SCALAR( JSON_EXTRACT(chips, '$[0].options'), '$[0].text' ) AS text
    
FROM
    source
)
