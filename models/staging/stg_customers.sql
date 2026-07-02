with cleaned as (

    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        email,
        upper(trim(country)) as country_clean,
        cast(signup_date as date) as signup_date
    from {{ ref('raw_customers') }}

)

select
    customer_id,
    first_name,
    last_name,
    email,
    case
        when country_clean in ('IT', 'ITALY', 'ITA') then 'IT'
        when country_clean in ('FR', 'FRANCE') then 'FR'
        when country_clean in ('NL', 'NETHERLANDS') then 'NL'
        when country_clean in ('DE', 'GERMANY') then 'DE'
        else null
    end as country,
    signup_date
from cleaned