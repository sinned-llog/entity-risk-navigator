{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with entity_master as (

    select
        entity_candidate_id,
        lei,
        legal_name,
        legal_name_normalized,
        country,
        direct_parent_lei,
        direct_parent_name,
        ultimate_parent_lei,
        ultimate_parent_name,
        has_direct_parent,
        has_ultimate_parent,
        source_load_date
    from {{ ref('mart_entity_master') }}

),

direct_parent_edges as (

    select
        md5(
            coalesce(lei, '') || '|direct_parent|' || coalesce(direct_parent_lei, '')
        ) as relationship_edge_id,

        entity_candidate_id as child_entity_candidate_id,
        lei as child_lei,
        legal_name as child_legal_name,
        legal_name_normalized as child_legal_name_normalized,
        country as child_country,

        direct_parent_lei as parent_lei,
        direct_parent_name as parent_name,

        'direct_parent' as relationship_type,
        1 as relationship_depth,

        source_load_date,
        now() as mart_loaded_at

    from entity_master
    where has_direct_parent = true
      and direct_parent_lei is not null

),

ultimate_parent_edges as (

    select
        md5(
            coalesce(lei, '') || '|ultimate_parent|' || coalesce(ultimate_parent_lei, '')
        ) as relationship_edge_id,

        entity_candidate_id as child_entity_candidate_id,
        lei as child_lei,
        legal_name as child_legal_name,
        legal_name_normalized as child_legal_name_normalized,
        country as child_country,

        ultimate_parent_lei as parent_lei,
        ultimate_parent_name as parent_name,

        'ultimate_parent' as relationship_type,

        case
            when direct_parent_lei is not null
             and ultimate_parent_lei is not null
             and direct_parent_lei <> ultimate_parent_lei
                then 2
            else 1
        end as relationship_depth,

        source_load_date,
        now() as mart_loaded_at

    from entity_master
    where has_ultimate_parent = true
      and ultimate_parent_lei is not null

),

final as (

    select *
    from direct_parent_edges

    union all

    select *
    from ultimate_parent_edges
    where parent_lei is distinct from child_lei

)

select *
from final