/*
Query to implement the method described in https://fingertips.phe.org.uk/documents/Outputs%20by%20ethnic%20group%20in%20CHIME.pdf
to summarise conflicting reported ethnicities into a single representative value.
Unknown ethnic groups = 99/X/Z
Other ethnic group = S
The approach is:
1. Use the most frequent ethnicity recorded across the all excluding any unknown values.
2. If there are multiple ethnicities in the data sets with the same frequency, the most recent is chosen.
3. If there are multiple ethnicities with the same frequency and latest date, precedence is given to the most recent value from the APC data set as it is considered more robust, followed by the AE data set, followed by the OP data set, followed by 100k dataset. Checks completed by NHS Digital indicate completeness in the AE data set is better than the OP data set.
4. If there are multiple ethnicities with the same frequency, latest date and source of data we select the ethnicity that occurs more frequently in the general population of England and Wales, according to the 2011 Census.
5. A value of ethnicity unknown will only be present if there are no known ethnicities in any of the HES data sets.
6. To take into the account the overrepresentation ofthe Other ethnic group, if the most common ethnic group assigned by the method above is Other:
  * The second most common usable ethnic group is assigned instead
  * If there are no other usable ethnic groups, the person is assigned to the Other ethnic group. A person will only be assigned to the Other ethnic group if there are no other usable ethnic groups

*/
create view ethnicity_store.vw_participant_ethnicity as
with best_valid_ethnicity as (
    with all_rows as (
        select re.participant_id
            ,re.ethnicity_cid 
            ,re.source_cid
            ,count(re.ethnicity_cid) as eth_count
            ,max(re.source_date) as max_source_date
        from ethnicity_store.reported_ethnicity re
        group by re.participant_id, re.ethnicity_cid , source_cid
        having re.ethnicity_cid not in (
            select uid from ethnicity_store.concept c
            where concept_code in ('S', 'Z', '99', 'X') and codesystem = 'reported_ethnicity_code'
            )
    ),
    source_priority as (
        select *
        from (values
            ('hes_apc', 1),
            ('dams', 2)
        ) as t("source", "rank")
    ),
    population_ethnicity as (
        select *
        from (values
            ('A', 1),
            ('C', 2),
            ('H', 3),
            ('J', 4),
            ('N', 5),
            ('L', 6),
            ('M', 7),
            ('B', 8),
            ('K', 9),
            ('D', 10),
            ('R', 11),
            ('F', 12),
            ('G', 13),
            ('P', 14),
            ('E', 15),
            ('S', 16)
        ) as t("ethnicity_code", "rank")
    )
    select ar.participant_id
        ,rec.concept_code as ethnicity_code
        ,row_number() over(partition by ar.participant_id order by ar.eth_count desc, ar.max_source_date desc, sp.rank asc, pe.rank asc) as row_rank
    from all_rows ar
    join ethnicity_store.concept sc 
        on ar.source_cid = sc.uid
    join ethnicity_store.concept rec 
        on ar.ethnicity_cid = rec.uid
    join source_priority sp 
        on sc.concept_code = sp.source
    join population_ethnicity pe 
        on rec.concept_code = pe.ethnicity_code
),
all_participants as (
    select re.participant_id
        ,bool_or(rec.concept_code = 'S') as got_other
        ,bool_or(rec.concept_code in ('99', 'X', 'Z')) as got_unknown
    from ethnicity_store.reported_ethnicity re
    join ethnicity_store.concept rec 
        on re.ethnicity_cid = rec.uid
    group by re.participant_id 
)
select ap.participant_id,
    case
        when bve.ethnicity_code is not null then bve.ethnicity_code
        when bve.ethnicity_code is null and ap.got_other = true then 'S'
        else '99'
    end as best_ethnicity_code
from all_participants ap 
left join (select * from best_valid_ethnicity where row_rank = 1) bve 
    on ap.participant_id = bve.participant_id
;

alter view ethnicity_store.vw_participant_ethnicity owner to cdt_user;
