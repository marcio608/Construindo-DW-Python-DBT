-- import

with source as (
    select 
        "Date", --" " é necessário pois o postgres coloca o nome da coluna com letra minúscula.
        "Close",
        simbolo
    from 
        {{ source ('postgres_dw', 'comnodities') }}
),

-- renamed

renamed as(

    select 
        cast("Date" as date) as data,
        "Close" as valor_fechamento,
        simbolo
    from source
)

--select * from

select * from renamed