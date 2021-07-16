--create extension if not exists "uuid-ossp";
create schema ethnicity_store;
alter schema ethnicity_store owner to cdt_user;

create table ethnicity_store.concept (
	uid uuid not null default uuid_generate_v4(),
	concept_code varchar not null,
	codesystem varchar not null,
	description varchar null,
	constraint concept_pkey primary key (uid),
	constraint concept_uq unique (concept_code, codesystem)
);

create table ethnicity_store.participant (
    id varchar not null,
    group_cid uuid,
    in_ngrl bool default false,
    programme_cid uuid not null,
    constraint participant_pkey primary key (id),
    constraint participant_group_cid_foreign_key foreign key (group_cid) references ethnicity_store.concept(uid),
    constraint participant_programme_cid_foreign_key foreign key (programme_cid) references ethnicity_store.concept(uid)
);

create table ethnicity_store.reported_ethnicity (
    participant_id varchar not null,
    ethnicity_cid uuid not null,
    source_cid uuid not null,
    source_date date not null,
    constraint participant_id_foreign_key foreign key (participant_id) references ethnicity_store.participant(id),
    constraint ethnicity_cid_foreign_key foreign key (ethnicity_cid) references ethnicity_store.concept(uid),
    constraint source_cid_foreign_key foreign key (source_cid) references ethnicity_store.concept(uid),
    primary key (participant_id, ethnicity_cid, source_cid, source_date)
);

create table ethnicity_store.predicted_ancestry (
    participant_id varchar not null,
    ancestry_cid uuid not null,
    prop numeric not null,
    constraint participant_id_foreign_key foreign key (participant_id) references ethnicity_store.participant(id),
    constraint ancestry_cid_foreign_key foreign key (ancestry_cid) references ethnicity_store.concept(uid)
);

alter table ethnicity_store.participant owner to cdt_user;
alter table ethnicity_store.concept owner to cdt_user;
alter table ethnicity_store.reported_ethnicity owner to cdt_user;
alter table ethnicity_store.predicted_ancestry owner to cdt_user;
