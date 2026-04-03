begin;

create table if not exists organizations (
    id bigint generated always as identity primary key,
    ein text not null unique,
    legal_name text not null,
    doing_business_as_name text,
    city text,
    state text,
    zip_code text,
    country text default 'US',
    ruling_month text,
    subsection_code text,
    foundation_code text,
    classification_code text,
    affiliation_code text,
    deductibility_code text,
    organization_type text,
    exempt_status_code text,
    tax_period text,
    ntee_code text,
    sort_name text,
    latest_registry_source text not null,
    latest_registry_updated_at timestamptz,
    raw_registry_payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists organizations_state_idx on organizations (state);
create index if not exists organizations_updated_at_idx on organizations (updated_at desc);

create table if not exists organization_status (
    id bigint generated always as identity primary key,
    ein text not null references organizations (ein) on delete cascade,
    source_name text not null,
    status_code text not null,
    status_label text,
    status_value text,
    effective_date text,
    observed_at timestamptz not null,
    is_current boolean not null default true,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    unique (ein, source_name, status_code, observed_at)
);

create index if not exists organization_status_current_idx
    on organization_status (ein, source_name, is_current, observed_at desc);

create table if not exists filing_index (
    id bigint generated always as identity primary key,
    object_id text not null unique,
    return_id text,
    ein text,
    tax_year integer,
    filing_year integer,
    tax_period text,
    form_type text,
    taxpayer_name text,
    submitted_on text,
    xml_url text not null,
    index_url text not null,
    source_updated_at timestamptz,
    filing_status text not null default 'discovered',
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists filing_index_ein_year_idx on filing_index (ein, tax_year);
create index if not exists filing_index_status_idx on filing_index (filing_status, filing_year);

create table if not exists raw_filings (
    id bigint generated always as identity primary key,
    object_id text not null unique references filing_index (object_id) on delete cascade,
    return_id text,
    ein text,
    xml_url text not null,
    index_url text,
    storage_provider text not null,
    storage_bucket text not null,
    storage_path text not null,
    artifact_checksum text not null,
    content_length bigint,
    content_type text,
    fetched_at timestamptz,
    fetch_status text not null default 'fetched',
    metadata jsonb not null default '{}'::jsonb,
    raw_metadata jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (storage_provider, storage_bucket, storage_path),
    unique (artifact_checksum)
);

create index if not exists raw_filings_ein_idx on raw_filings (ein);
create index if not exists raw_filings_status_idx on raw_filings (fetch_status, fetched_at desc);

create table if not exists normalized_filings (
    id bigint generated always as identity primary key,
    object_id text not null references filing_index (object_id) on delete cascade,
    return_id text,
    ein text,
    tax_year integer,
    filing_year integer,
    tax_period text,
    form_type text,
    organization_name text,
    address_line_1 text,
    address_line_2 text,
    city text,
    state text,
    zip_code text,
    country text,
    organization_type text,
    deductibility_status text,
    public_charity_status text,
    total_revenue numeric(18,2),
    total_expenses numeric(18,2),
    total_assets numeric(18,2),
    total_liabilities numeric(18,2),
    net_assets numeric(18,2),
    mission_text text,
    employee_count integer,
    volunteer_count integer,
    contributions_revenue numeric(18,2),
    program_service_revenue numeric(18,2),
    investment_income numeric(18,2),
    program_service_accomplishments jsonb not null default '[]'::jsonb,
    officers jsonb not null default '[]'::jsonb,
    extracted_sections jsonb not null default '{}'::jsonb,
    narrative_sections jsonb not null default '{}'::jsonb,
    parser_version text not null,
    extracted_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (object_id, parser_version)
);

create index if not exists normalized_filings_ein_idx on normalized_filings (ein, filing_year);
create index if not exists normalized_filings_parser_idx on normalized_filings (parser_version, extracted_at desc);

create table if not exists enrichment_runs (
    id bigint generated always as identity primary key,
    object_id text not null references filing_index (object_id) on delete cascade,
    normalized_filing_id bigint not null references normalized_filings (id) on delete cascade,
    prompt_version text not null,
    model_name text not null,
    input_hash text not null,
    request_payload jsonb not null default '{}'::jsonb,
    response_payload jsonb not null default '{}'::jsonb,
    token_input integer not null default 0,
    token_output integer not null default 0,
    estimated_cost_usd numeric(12,6) not null default 0,
    run_status text not null,
    error_message text,
    completed_at timestamptz,
    created_at timestamptz not null default now()
);

create index if not exists enrichment_runs_lookup_idx
    on enrichment_runs (object_id, prompt_version, model_name, completed_at desc);
create index if not exists enrichment_runs_hash_idx on enrichment_runs (input_hash);

create table if not exists nonprofit_profiles (
    id bigint generated always as identity primary key,
    object_id text not null references filing_index (object_id) on delete cascade,
    ein text,
    normalized_filing_id bigint not null references normalized_filings (id) on delete cascade,
    enrichment_run_id bigint references enrichment_runs (id) on delete set null,
    prompt_version text not null,
    model_name text not null,
    profile_summary text,
    cause_tags jsonb not null default '[]'::jsonb,
    program_highlights jsonb not null default '[]'::jsonb,
    location_hints jsonb not null default '[]'::jsonb,
    fit_notes jsonb not null default '[]'::jsonb,
    derived_profile jsonb not null default '{}'::jsonb,
    source_text_hash text not null,
    is_current boolean not null default true,
    output_schema_version text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (object_id, prompt_version, model_name, source_text_hash)
);

create index if not exists nonprofit_profiles_current_idx on nonprofit_profiles (is_current, updated_at desc);
create index if not exists nonprofit_profiles_ein_idx on nonprofit_profiles (ein);

create table if not exists embeddings_index_status (
    id bigint generated always as identity primary key,
    object_id text not null references filing_index (object_id) on delete cascade,
    nonprofit_profile_id bigint not null references nonprofit_profiles (id) on delete cascade,
    adapter_name text not null,
    document_hash text not null,
    status text not null,
    indexed_at timestamptz,
    attempt_count integer not null default 0,
    last_error text,
    payload jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (object_id, adapter_name, document_hash)
);

create index if not exists embeddings_status_idx
    on embeddings_index_status (adapter_name, status, updated_at desc);

create table if not exists job_runs (
    id bigint generated always as identity primary key,
    job_type text not null,
    status text not null,
    payload jsonb not null default '{}'::jsonb,
    progress_payload jsonb not null default '{}'::jsonb,
    result_payload jsonb not null default '{}'::jsonb,
    idempotency_key text not null,
    priority integer not null default 100,
    attempt_count integer not null default 0,
    max_attempts integer not null default 5,
    run_after timestamptz not null default now(),
    claimed_by text,
    claimed_at timestamptz,
    claimed_until timestamptz,
    completed_at timestamptz,
    last_error text,
    last_error_at timestamptz,
    dead_lettered_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (job_type, idempotency_key)
);

create index if not exists job_runs_claim_idx
    on job_runs (job_type, status, run_after, claimed_until, priority, id);
create index if not exists job_runs_dead_letter_idx
    on job_runs (job_type, dead_lettered_at desc)
    where dead_lettered_at is not null;

commit;
