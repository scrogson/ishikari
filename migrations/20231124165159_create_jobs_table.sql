create type job_state as enum (
    'available',
    'scheduled',
    'executing',
    'retryable',
    'completed',
    'discarded',
    'cancelled'
);

create table jobs (
    id bigserial primary key,
    state job_state not null default 'available'::job_state,
    queue text not null default 'default'::text check (char_length(queue) > 0 and char_length(queue) < 128),
    worker text not null check (char_length(worker) > 0 and char_length(worker) < 128),
    args jsonb not null default '{}'::jsonb,
    errors jsonb[] not null default array[]::jsonb[],
    attempt integer not null default 0,
    max_attempts integer not null default 20 check (max_attempts > 0),
    attempted_by text[],
    priority integer not null default 0 check (priority >= 0 and priority <= 3),
    tags character varying(255)[] default array[]::character varying[],
    meta jsonb default '{}'::jsonb,
    inserted_at timestamptz not null default now(),
    scheduled_at timestamptz not null default now(),
    attempted_at timestamptz,
    completed_at timestamptz,
    discarded_at timestamptz,
    cancelled_at timestamptz,
    constraint attempt_range check (attempt >= 0 and attempt <= max_attempts)
);
comment on table jobs is '1';

-- Indices -------------------------------------------------------

create index jobs_state_queue_priority_scheduled_at_id_index on jobs(state enum_ops, queue text_ops, priority int4_ops, scheduled_at timestamptz_ops, id int8_ops);
create index jobs_args_index on jobs using gin (args jsonb_ops);
create index jobs_meta_index on jobs using gin (meta jsonb_ops);

-- -- Functions -------------------------------------------------------
create or replace function jobs_notify()
returns trigger AS $$
declare
    channel text;
    notice json;
begin
    if new.state = 'available' then
        channel := 'insert';
        notice := json_build_object('queue', new.queue);

        perform pg_notify(channel, notice::text);
    end if;

    return null;
end;
$$ language plpgsql;

-- -- Triggers -------------------------------------------------------

create trigger ishikari_notify
  after insert on jobs
  for each row
  execute function jobs_notify();
