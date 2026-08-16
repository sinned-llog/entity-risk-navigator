{{ config(
    materialized = 'table',
    schema = 'marts'
) }}

with recursive entity_master as (

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
    where lei is not null

),

parent_walk as (

    select
        child.entity_candidate_id as root_entity_candidate_id,
        child.lei as root_lei,
        child.legal_name as root_legal_name,
        child.legal_name_normalized as root_legal_name_normalized,
        child.country as root_country,

        child.direct_parent_lei as ancestor_lei,
        child.direct_parent_name as ancestor_name,

        1 as relationship_depth,

        array[child.lei, child.direct_parent_lei] as lei_path,
        child.lei || ' > ' || child.direct_parent_lei as lei_path_text,

        child.source_load_date

    from entity_master child
    where child.direct_parent_lei is not null
      and child.direct_parent_lei <> child.lei

    union all

    select
        pw.root_entity_candidate_id,
        pw.root_lei,
        pw.root_legal_name,
        pw.root_legal_name_normalized,
        pw.root_country,

        parent.direct_parent_lei as ancestor_lei,
        parent.direct_parent_name as ancestor_name,

        pw.relationship_depth + 1 as relationship_depth,

        pw.lei_path || parent.direct_parent_lei as lei_path,
        pw.lei_path_text || ' > ' || parent.direct_parent_lei as lei_path_text,

        pw.source_load_date

    from parent_walk pw
    inner join entity_master parent
        on pw.ancestor_lei = parent.lei
    where parent.direct_parent_lei is not null
      and parent.direct_parent_lei <> parent.lei
      and not parent.direct_parent_lei = any(pw.lei_path)
      and pw.relationship_depth < 10

),

final as (

    select
        md5(
            coalesce(root_lei, '') || '|path|' ||
            coalesce(ancestor_lei, '') || '|' ||
            relationship_depth::text
        ) as relationship_path_id,

        root_entity_candidate_id,
        root_lei,
        root_legal_name,
        root_legal_name_normalized,
        root_country,

        ancestor_lei,
        ancestor_name,
        relationship_depth,

        lei_path_text,

        case
            when relationship_depth = max(relationship_depth) over (
                partition by root_entity_candidate_id
            )
                then true
            else false
        end as is_furthest_known_ancestor,

        source_load_date,
        now() as mart_loaded_at

    from parent_walk

)

select *
from final