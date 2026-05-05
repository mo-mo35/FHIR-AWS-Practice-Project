select
    code_text as condition,
    count(*) as occurrences
from {{ source('fhir', 'conditions') }}
where code_text is not null
group by code_text
order by occurrences desc
limit 10