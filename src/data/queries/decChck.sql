create table if not exists {target_table} as (
    select a.{id}, a.{name}, a.{code}, a.{date}, a.{hour}, f.{a}, f.{b}, a.{temperature}, a.{declare}, (f.{a} * a.{temperature} + f.{b}) as calculated
    from {aggregation_table} as a join {factor_table} as f
    on a.{id} = f.{id} and a.{code} = f.{code} where a.{status_type} = 'SO' order by date asc, hour asc
)