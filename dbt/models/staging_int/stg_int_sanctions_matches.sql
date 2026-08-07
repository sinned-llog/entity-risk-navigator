{{ config(
    materialized='table',
    schema='staging',
    alias='stg_int_entity_sanctions_matches',
    post_hook=[
      "CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_id ON {{ this }} (entity_sanctions_match_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_entity ON {{ this }} (entity_candidate_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_lei ON {{ this }} (lei);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_subject ON {{ this }} (sanction_subject_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_name ON {{ this }} (sanctions_name_id);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_source ON {{ this }} (sanctions_source);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_match_key ON {{ this }} (entity_name_match_key, sanctions_name_match_key);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_quality ON {{ this }} (match_quality_tier);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_flags ON {{ this }} (is_short_match_key, is_generic_match_key, has_country_overlap, has_identifier_overlap);",
      "CREATE INDEX IF NOT EXISTS idx_stg_int_entity_sanctions_matches_load_date ON {{ this }} (source_load_date);"
    ]
) }}

with entity_candidates as (

    select
        entity_candidate_id,
        candidate_source,
        candidate_source_id,

        lei,
        legal_name,
        legal_name_normalized,

        entity_status,
        registration_status,

        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        country,

        nullif(
            trim(
                regexp_replace(
                    regexp_replace(
                        lower(trim(coalesce(legal_name_normalized, ''))),
                        '[[:punct:]]+',
                        ' ',
                        'g'
                    ),
                    '[[:space:]]+',
                    ' ',
                    'g'
                )
            ),
            ''
        ) as entity_name_match_key,

        source_load_date as entity_source_load_date,
        source_object_key as entity_source_object_key,
        raw_id as entity_raw_id,
        row_hash as entity_row_hash

    from {{ ref('stg_int_entity_candidates') }}

    where legal_name_normalized is not null
      and trim(legal_name_normalized) <> ''

),

sanctions_names as (

    select
        sanctions_name_id,
        sanction_subject_id,

        sanctions_source,
        source_name_id,
        source_subject_id,
        source_entity_key,

        entity_type as sanctions_entity_type,
        entity_subject_type as sanctions_entity_subject_type,

        source_name_type,
        is_primary_name,

        name as sanctions_name,
        name_normalized as sanctions_name_normalized,
        name_match_key as sanctions_name_match_key,
        has_non_ascii,

        source_load_date as sanctions_name_source_load_date,
        source_object_key as sanctions_name_source_object_key,
        raw_id as sanctions_name_raw_id,
        row_hash as sanctions_name_row_hash

    from {{ ref('stg_int_sanctions_names_unified') }}

    where name_match_key is not null
      and trim(name_match_key) <> ''
      and entity_type in (
          'enterprise',
          'organization',
          'company',
          'legalentity'
      )

),

name_matches_narrow as (

    select
        e.entity_candidate_id,
        e.candidate_source,
        e.candidate_source_id,

        e.lei,
        e.legal_name,
        e.legal_name_normalized,
        e.entity_name_match_key,

        e.entity_status,
        e.registration_status,
        e.legal_jurisdiction,
        e.legal_address_country,
        e.headquarters_address_country,
        e.country,

        s.sanctions_name_id,
        s.sanction_subject_id,

        s.sanctions_source,
        s.source_name_id,
        s.source_subject_id,
        s.source_entity_key,

        s.sanctions_entity_type,
        s.sanctions_entity_subject_type,

        s.source_name_type,
        s.is_primary_name,

        s.sanctions_name,
        s.sanctions_name_normalized,
        s.sanctions_name_match_key,
        s.has_non_ascii,

        'exact_name_match_key' as match_type,
        100::integer as match_score,

        e.entity_source_load_date,
        s.sanctions_name_source_load_date,

        e.entity_source_object_key,
        s.sanctions_name_source_object_key,

        e.entity_raw_id,
        s.sanctions_name_raw_id,

        e.entity_row_hash,
        s.sanctions_name_row_hash

    from entity_candidates e

    inner join sanctions_names s
        on e.entity_name_match_key = s.sanctions_name_match_key

    where e.entity_name_match_key is not null
      and trim(e.entity_name_match_key) <> ''
      and length(e.entity_name_match_key) >= 2

),

ranked_matches as (

    select
        name_matches_narrow.*,

        row_number() over (
            partition by
                entity_candidate_id,
                sanction_subject_id,
                entity_name_match_key
            order by
                is_primary_name desc,
                case
                    when source_name_type = 'primary_name' then 1
                    when source_name_type = 'name_alias' then 2
                    when source_name_type = 'alias_name' then 3
                    else 9
                end asc,
                sanctions_source asc,
                source_name_id asc
        ) as match_rank

    from name_matches_narrow

),

deduped_matches as (

    select
        *
    from ranked_matches
    where match_rank = 1

),

sanction_subjects as (

    select
        sanction_subject_id,
        sanctions_source,
        source_subject_id,
        source_entity_key,

        entity_type as subject_entity_type,
        entity_subject_type as subject_entity_subject_type,

        primary_name,
        primary_name_normalized,

        is_sanctioned,

        source_sanction_context,
        source_programs,
        source_lists,
        source_datasets,
        source_reference_numbers,
        source_reference_date,
        source_reference_url,
        source_countries,
        source_identifiers,
        source_context_raw,

        source_first_seen_raw,
        source_last_seen_raw,
        source_last_change_raw,

        source_load_date as subject_source_load_date,
        source_object_key as subject_source_object_key,
        metadata_object_key as subject_metadata_object_key,
        raw_id as subject_raw_id,
        row_hash as subject_row_hash

    from {{ ref('stg_int_sanctions_subjects_unified') }}

),

matches_with_context as (

    select
        d.*,

        subj.primary_name as sanctions_subject_primary_name,
        subj.primary_name_normalized as sanctions_subject_primary_name_normalized,

        subj.is_sanctioned,

        subj.source_sanction_context,
        subj.source_programs,
        subj.source_lists,
        subj.source_datasets,
        subj.source_reference_numbers,
        subj.source_reference_date,
        subj.source_reference_url,
        subj.source_countries,
        subj.source_identifiers,
        subj.source_context_raw,

        subj.source_first_seen_raw,
        subj.source_last_seen_raw,
        subj.source_last_change_raw,

        subj.subject_source_load_date,
        subj.subject_source_object_key,
        subj.subject_metadata_object_key,
        subj.subject_raw_id,
        subj.subject_row_hash

    from deduped_matches d

    left join sanction_subjects subj
        on d.sanction_subject_id = subj.sanction_subject_id

),

quality_flags as (

    select
        *,

        length(entity_name_match_key) as entity_name_match_key_length,

        case
            when length(entity_name_match_key) <= 3 then true
            else false
        end as is_short_match_key,

        case
            when length(entity_name_match_key) <= 2 then true
            else false
        end as is_very_short_match_key,

        case
            when entity_name_match_key in (
                'llc',
                'ltd',
                'limited',
                'inc',
                'corp',
                'corporation',
                'company',
                'co',
                'bank',
                'group',
                'holding',
                'holdings',
                'investment',
                'invest',
                'capital',
                'finance',
                'fund',
                'trust',
                'rt',
                'ana',
                'hcg',
                'isb',
                'clara',
                'jules',
                'kera',
                'zenit',
                'pegasus',
                'matsa',
                'homa',
                'vsc',
                'spm',
                'tsa'
            ) then true
            else false
        end as is_generic_match_key,

        case
            when country is not null
             and trim(country) <> ''
             and source_countries is not null
             and trim(source_countries) <> ''
                then true
            else false
        end as has_country_information,

        case
            when country is not null
             and trim(country) <> ''
             and source_countries is not null
             and trim(source_countries) <> ''
             and country = any(string_to_array(source_countries, ';'))
                then true
            else false
        end as has_country_overlap,

        case
            when country is null
              or trim(country) = ''
                then 'entity_country_missing'
            when source_countries is null
              or trim(source_countries) = ''
                then 'sanctions_country_missing'
            when country = any(string_to_array(source_countries, ';'))
                then 'country_overlap'
            else 'country_mismatch'
        end as country_overlap_status,

        case
            when source_identifiers is not null
             and trim(source_identifiers) <> ''
             and (
                    position(upper(lei) in upper(source_identifiers)) > 0
                 or position(upper(candidate_source_id) in upper(source_identifiers)) > 0
             )
                then true
            else false
        end as has_identifier_overlap,

        case
            when source_name_type = 'primary_name' then true
            else false
        end as is_primary_name_match,

        case
            when source_name_type in ('alias_name', 'name_alias') then true
            else false
        end as is_alias_name_match

    from matches_with_context

),

scored_matches as (

    select
        *,

        case
            when has_identifier_overlap = true
                then 100

            when is_primary_name_match = true
             and has_country_overlap = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 95

            when is_primary_name_match = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 90

            when is_alias_name_match = true
             and has_country_overlap = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 85

            /*
              EU FSF does not currently provide source_countries in our unified
              subject model. Therefore exact EU FSF alias/name_alias matches
              should not be automatically downgraded to review_required only
              because country context is missing.
            */
            when sanctions_source = 'eu_fsf'
             and is_alias_name_match = true
             and country_overlap_status = 'sanctions_country_missing'
             and is_short_match_key = false
             and is_generic_match_key = false
                then 80

            when is_short_match_key = true
              or is_generic_match_key = true
                then 50

            else 70
        end as match_quality_score,

        case
            when has_identifier_overlap = true
                then 'high_confidence'

            when is_primary_name_match = true
             and has_country_overlap = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 'high_confidence'

            when is_primary_name_match = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 'medium_confidence'

            when is_alias_name_match = true
             and has_country_overlap = true
             and is_short_match_key = false
             and is_generic_match_key = false
                then 'medium_confidence'

            /*
              EU FSF-specific rule:
              Missing sanctions country is expected for EU FSF in the current
              model, so a clean exact EU FSF alias/name_alias match can be
              medium_confidence instead of review_required.
            */
            when sanctions_source = 'eu_fsf'
             and is_alias_name_match = true
             and country_overlap_status = 'sanctions_country_missing'
             and is_short_match_key = false
             and is_generic_match_key = false
                then 'medium_confidence'

            when is_short_match_key = true
              or is_generic_match_key = true
                then 'review_required'

            when country_overlap_status = 'country_mismatch'
                then 'review_required'

            else 'review_required'
        end as match_quality_tier,

        concat_ws(
            '; ',
            case
                when has_identifier_overlap = true
                    then 'identifier_overlap'
            end,
            case
                when has_country_overlap = true
                    then 'country_overlap'
            end,
            case
                when country_overlap_status = 'country_mismatch'
                    then 'country_mismatch'
            end,
            case
                when country_overlap_status = 'entity_country_missing'
                    then 'entity_country_missing'
            end,
            case
                when country_overlap_status = 'sanctions_country_missing'
                    then 'sanctions_country_missing'
            end,
            case
                when sanctions_source = 'eu_fsf'
                 and country_overlap_status = 'sanctions_country_missing'
                    then 'eu_fsf_country_context_not_available'
            end,
            case
                when is_short_match_key = true
                    then 'short_match_key'
            end,
            case
                when is_generic_match_key = true
                    then 'generic_match_key'
            end,
            case
                when is_primary_name_match = true
                    then 'primary_name_match'
            end,
            case
                when is_alias_name_match = true
                    then 'alias_name_match'
            end
        ) as match_quality_reasons

    from quality_flags

),

final as (

    select
        md5(
            concat_ws(
                '|',
                entity_candidate_id,
                sanction_subject_id,
                entity_name_match_key,
                match_type
            )
        ) as entity_sanctions_match_id,

        entity_candidate_id,
        candidate_source,
        candidate_source_id,

        lei,
        legal_name,
        legal_name_normalized,
        entity_name_match_key,
        entity_name_match_key_length,

        entity_status,
        registration_status,
        legal_jurisdiction,
        legal_address_country,
        headquarters_address_country,
        country,

        sanctions_name_id,
        sanction_subject_id,

        sanctions_source,
        source_name_id,
        source_subject_id,
        source_entity_key,

        sanctions_entity_type,
        sanctions_entity_subject_type,

        source_name_type,
        is_primary_name,

        sanctions_name,
        sanctions_name_normalized,
        sanctions_name_match_key,
        has_non_ascii,

        sanctions_subject_primary_name,
        sanctions_subject_primary_name_normalized,

        is_sanctioned,

        source_sanction_context,
        source_programs,
        source_lists,
        source_datasets,
        source_reference_numbers,
        source_reference_date,
        source_reference_url,
        source_countries,
        source_identifiers,
        source_context_raw,

        source_first_seen_raw,
        source_last_seen_raw,
        source_last_change_raw,

        match_type,
        match_score,
        match_rank,

        match_quality_score,
        match_quality_tier,
        match_quality_reasons,

        is_short_match_key,
        is_very_short_match_key,
        is_generic_match_key,
        has_country_information,
        has_country_overlap,
        country_overlap_status,
        has_identifier_overlap,
        is_primary_name_match,
        is_alias_name_match,

        greatest(
            entity_source_load_date,
            sanctions_name_source_load_date,
            subject_source_load_date
        ) as source_load_date,

        entity_source_load_date,
        sanctions_name_source_load_date,
        subject_source_load_date,

        entity_source_object_key,
        sanctions_name_source_object_key,
        subject_source_object_key,
        subject_metadata_object_key,

        entity_raw_id,
        sanctions_name_raw_id,
        subject_raw_id,

        entity_row_hash,
        sanctions_name_row_hash,
        subject_row_hash,

        current_timestamp as intermediate_loaded_at

    from scored_matches

)

select *
from final