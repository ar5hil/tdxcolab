create table if not exists public.tdl_messages (
    id bigserial primary key,
    chat_id bigint not null,
    topic_id integer not null default 0,
    message_id integer not null,
    export_seq bigint not null,
    created_at timestamptz not null default now(),
    unique (chat_id, topic_id, message_id)
);

create index if not exists tdl_messages_chat_topic_seq_idx
    on public.tdl_messages (chat_id, topic_id, export_seq);
