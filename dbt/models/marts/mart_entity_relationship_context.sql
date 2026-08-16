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
        has_any_parent,
        source_load_date
    from {{ ref('mart_entity_master') }}

),

child_counts as (

    select
        parent_lei,
        count(distinct child_lei) as known_child_count
    from {{ ref('mart_entity_relationship_edges') }}
    where relationship_type = 'direct_parent'
      and parent_lei is not null
    group by parent_lei

),

descendant_counts as (

    select
        ancestor_lei as parent_lei,
        count(distinct root_lei) as total_descendant_count
    from {{ ref('mart_entity_parent_paths') }}
    where ancestor_lei is not null
      and root_lei is not null
      and ancestor_lei <> root_lei
    group by ancestor_lei

),

same_parent_counts as (

    select
        direct_parent_lei,
        count(*) as same_parent_group_size
    from entity_master
    where direct_parent_lei is not null
    group by direct_parent_lei

),

path_summary as (

    select
        root_entity_candidate_id as entity_candidate_id,
        max(relationship_depth) as parent_chain_depth,
        count(*) as parent_path_count,
        max(case when is_furthest_known_ancestor then ancestor_lei end) as furthest_known_ancestor_lei,
        max(case when is_furthest_known_ancestor then ancestor_name end) as furthest_known_ancestor_name
    from {{ ref('mart_entity_parent_paths') }}
    group by root_entity_candidate_id

),

final as (

    select
        e.entity_candidate_id,
        e.lei,
        e.legal_name,
        e.legal_name_normalized,
        e.country,

        e.direct_parent_lei,
        e.direct_parent_name,
        e.ultimate_parent_lei,
        e.ultimate_parent_name,

        e.has_direct_parent,
        e.has_ultimate_parent,
        e.has_any_parent,

        coalesce(c.known_child_count, 0) as known_child_count,
        coalesce(dc.total_descendant_count, 0) as total_descendant_count,

        case
            when e.direct_parent_lei is null then 0
            else greatest(coalesce(sp.same_parent_group_size, 0) - 1, 0)
        end as same_parent_entity_count,

        coalesce(ps.parent_chain_depth, 0) as parent_chain_depth,
        coalesce(ps.parent_path_count, 0) as parent_path_count,

        ps.furthest_known_ancestor_lei,
        ps.furthest_known_ancestor_name,

        case
            when coalesce(c.known_child_count, 0) > 0 then true
            else false
        end as has_known_children,

        case
            when e.has_any_parent = false and coalesce(dc.total_descendant_count, 0) = 0
                then 'No known parent or child relationships.'
            when e.has_any_parent = true and coalesce(dc.total_descendant_count, 0) = 0
                then 'Entity has known parent relationship context.'
            when e.has_any_parent = false and coalesce(dc.total_descendant_count, 0) > 0
                then 'Entity has known child or descendant entities but no known parent.'
            else 'Entity has both parent and child or descendant relationship context.'
        end as relationship_context_summary,

        e.source_load_date,
        now() as mart_loaded_at

    from entity_master e
    left join child_counts c
        on e.lei = c.parent_lei
    left join same_parent_counts sp
        on e.direct_parent_lei = sp.direct_parent_lei
    left join path_summary ps
        on e.entity_candidate_id = ps.entity_candidate_id
    left join descendant_counts dc
    on e.lei = dc.parent_lei

)

select *
from final