

{#- Column data types used for both config + final cast -#}
{%- set col_types = {
    "dim_store_sk":       "bigint",
    "store_code":         "varchar(16)",
    "store_name":         "varchar(255)",
    "channel":            "varchar(32)",
    "address_line1":      "varchar(255)",
    "address_line2":      "varchar(255)",
    "city":               "varchar(128)",
    "county":             "varchar(128)",
    "postcode":           "varchar(10)",
    "country":            "varchar(3)",
    "latitude":           "double",
    "longitude":          "double",
    "manager_name":       "varchar(255)",
    "area_manager_name":  "varchar(255)",
    "store_size_sqft":    "bigint",
    "date_opened":        "date",
    "date_closed":        "date", 
    "trading_hours":      "varchar(255)",
    "region":             "varchar(64)",
    "concept":            "varchar(64)",
    "is_flagship":        "varchar(1)",    
    "is_franchise":       "varchar(1)",    
    "tax_region_code":    "varchar(16)",
    "timezone":           "varchar(64)",
    "row_hash":           "varchar(32)",
    "start_ts":           "timestamptz",
    "end_ts":             "timestamptz",
    "version_number":     "integer",
    "current_flag":       "boolean",
    "prior_dim_store_sk": "bigint"
} -%}


{{ config(
    materialized='incremental',
    unique_key='dim_store_sk',
    schema='det',
    column_types= col_types,
    tags=['stores'],
    pre_hook = ["create sequence if not exists det.seq_dim_stores_sk start 1 increment 1"]

) }}

{%- set sequence_name = "nextval('det.seq_dim_stores_sk')" -%}
{%- set business_key = 'store_code' -%}

{#- compile-time constant for the run start timestamp (truncated to seconds). -#}
{%- set start_ts = "'" ~ run_started_at.strftime('%Y-%m-%d %H:%M:%S') ~ "'" -%}
{%- set end_ts = "'" ~ (run_started_at - modules.datetime.timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S') ~ "'" -%}
{%- set high_date_ts = '9999-12-31 00:00:00' -%}

{%- set surrogate_key = config.get('unique_key') -%}
{%- set prior_surrogate_key = 'prior_' ~ surrogate_key -%}

{# Columns used to compute the row hash #}
{%- set scd2_audit_cols = ['row_hash', 'start_ts','end_ts', 'version_number', 'current_flag', prior_surrogate_key]-%}

{%- set passthrough_columns = col_types.keys()
    | reject('equalto', surrogate_key)
    | reject('in', scd2_audit_cols )
    | list
-%}


with source as (

    select 
        {% for col_name in passthrough_columns %}
            {{ col_name }},
        {%- endfor %}
        -- Row change hash 
        {{ dbt_utils.generate_surrogate_key(passthrough_columns) }} as row_hash

    from {{ ref('t_stg_stores') }}

),
{% if is_incremental() %}

existing_records as (
    select  
	    {{ surrogate_key }},
        {#- passthrough columns -#}
        {% for col_name in passthrough_columns %}
            {{ col_name }},
        {%- endfor %}
        {#- scd2 audit columns -#}
        {%- for col_name in scd2_audit_cols %}
            {{ col_name }}{% if not loop.last %},        
            {%- endif -%}
        {%- endfor %}             

    from {{ this }}
    where   current_flag = true
),

changes as (
    select      
        {{ sequence_name }} as {{ surrogate_key }},
        {#- passthrough columns -#}
        {% for col_name in passthrough_columns %}
            c.{{ col_name }},
        {%- endfor %}    
		c.row_hash,
		{{ start_ts }}::timestamptz as start_ts,
		{{ high_date_ts }}::timestamptz as end_ts,        
		coalesce(e.version_number, 0) + 1 as version_number,
        true as current_flag,
        e.{{ surrogate_key }} as {{prior_surrogate_key}}
    from source as c
    left join existing_records as e
    on c.{{ business_key }} = e.{{ business_key }} 
    where 
	    e.{{ business_key }} is null
    or  c.row_hash != e.row_hash
    order by 
	    c.{{ business_key }} 
),

updates as (
    select      
	    e.{{ surrogate_key }},
        {#- passthrough columns -#}
        {% for col_name in passthrough_columns %}
            e.{{ col_name }},
        {%- endfor %}           
		e.row_hash,
		e.start_ts,
		{{ end_ts }} as end_ts,
		e.version_number,
		false as current_flag,
		e.{{prior_surrogate_key}}
    from existing_records e
    join changes c
    on e.{{ business_key }}  = c.{{ business_key }} 
    where       
	   e.current_flag = true
),

final as (

    select * from changes
    union all
    select * from updates
)
{% else %}

final as (

select  
    {{ sequence_name }} as {{ surrogate_key }},
    {{ business_key }},
    {#- passthrough columns -#}
    {% for col_name in passthrough_columns %}
        {{ col_name }},
    {%- endfor %} 
	row_hash,
	{{ start_ts }}::timestamptz as start_ts,
	{{ high_date_ts }}::timestamptz as end_ts,
	1::integer as version_number,
	true as current_flag,
	-1::integer as {{prior_surrogate_key}}

from source

order by 
   {{ business_key }} 
)
{% endif %}

select
{%- for col_name, dtype in col_types.items() %}
    ({{ col_name }}::{{ dtype }}) as {{ col_name }}{% if not loop.last %},        
    {%- endif -%}
{%- endfor %}
from final


