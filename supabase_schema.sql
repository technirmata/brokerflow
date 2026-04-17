-- JP BrokerFlow Supabase Schema
-- Run this in Supabase SQL Editor after creating a new project.
-- Enables Row Level Security so each user only sees their own data.

-- =============================================================
-- user_configs — per-user API keys and workspace config
-- =============================================================
create table if not exists public.user_configs (
  user_id uuid primary key references auth.users(id) on delete cascade,

  -- ClickUp
  clickup_token text,
  clickup_workspace_id text,
  clickup_space_id text,
  clickup_folder_id text,
  clickup_list_active_deals text,
  clickup_list_brokers text,
  clickup_list_followups text,
  clickup_list_templates text,
  clickup_list_touchpoints text,

  -- Email — Gmail OAuth
  gmail_refresh_token text,
  gmail_email text,

  -- Email — SMTP fallback (Outlook, custom providers)
  smtp_host text,
  smtp_port int,
  smtp_user text,
  smtp_pass text,
  smtp_from text,

  -- SMS — Twilio
  twilio_account_sid text,
  twilio_auth_token text,
  twilio_from_number text,

  -- AI — Anthropic
  anthropic_api_key text,

  -- Onboarding state
  wizard_completed boolean default false,
  wizard_step int default 1,

  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

alter table public.user_configs enable row level security;

create policy "user_configs: owner read"
  on public.user_configs for select
  using (auth.uid() = user_id);

create policy "user_configs: owner insert"
  on public.user_configs for insert
  with check (auth.uid() = user_id);

create policy "user_configs: owner update"
  on public.user_configs for update
  using (auth.uid() = user_id);

-- =============================================================
-- touchpoints — every email/sms/call logged here
-- Indexed by broker + deal for fast activity timelines
-- =============================================================
create table if not exists public.touchpoints (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,

  -- Identity of the counterparty (always present)
  broker_id text not null,          -- ClickUp broker task id
  broker_name text,
  broker_email text,
  broker_phone text,

  -- Optional deal context
  deal_id text,                      -- ClickUp deal task id
  deal_name text,

  -- Communication details
  channel text not null check (channel in ('email','sms','call','note')),
  direction text not null check (direction in ('outbound','inbound','note')),
  subject text,
  body text,
  duration_seconds int,              -- for calls
  external_id text,                  -- Gmail message id, Twilio sid, etc.

  -- Source of the log entry
  source text default 'manual' check (source in ('manual','dashboard','gmail_scan','twilio_webhook','smtp')),

  occurred_at timestamptz not null default now(),
  created_at timestamptz default now()
);

create index if not exists idx_touchpoints_user_broker on public.touchpoints(user_id, broker_id, occurred_at desc);
create index if not exists idx_touchpoints_user_deal on public.touchpoints(user_id, deal_id, occurred_at desc);
create index if not exists idx_touchpoints_user_occurred on public.touchpoints(user_id, occurred_at desc);

alter table public.touchpoints enable row level security;

create policy "touchpoints: owner read"
  on public.touchpoints for select
  using (auth.uid() = user_id);

create policy "touchpoints: owner insert"
  on public.touchpoints for insert
  with check (auth.uid() = user_id);

create policy "touchpoints: owner update"
  on public.touchpoints for update
  using (auth.uid() = user_id);

create policy "touchpoints: owner delete"
  on public.touchpoints for delete
  using (auth.uid() = user_id);

-- =============================================================
-- deal_broker_links — explicit many-to-many between deals and brokers
-- (ClickUp's single broker_id field is limiting; some deals involve 2+ brokers)
-- =============================================================
create table if not exists public.deal_broker_links (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  deal_id text not null,
  broker_id text not null,
  role text default 'primary' check (role in ('primary','co-listing','buyer_rep','other')),
  created_at timestamptz default now(),
  unique(user_id, deal_id, broker_id)
);

create index if not exists idx_deal_broker_links_user_deal on public.deal_broker_links(user_id, deal_id);
create index if not exists idx_deal_broker_links_user_broker on public.deal_broker_links(user_id, broker_id);

alter table public.deal_broker_links enable row level security;

create policy "deal_broker_links: owner all"
  on public.deal_broker_links for all
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

-- =============================================================
-- email_threads_cache — cache of Gmail scan results so we don't hit quota
-- =============================================================
create table if not exists public.email_threads_cache (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references auth.users(id) on delete cascade,
  broker_email text not null,
  last_scanned_at timestamptz default now(),
  last_message_at timestamptz,
  last_message_subject text,
  last_message_direction text,
  message_count_30d int default 0,
  unique(user_id, broker_email)
);

create index if not exists idx_email_threads_user_email on public.email_threads_cache(user_id, broker_email);

alter table public.email_threads_cache enable row level security;

create policy "email_threads_cache: owner all"
  on public.email_threads_cache for all
  using (auth.uid() = user_id)
  with check (auth.uid() = user_id);

-- =============================================================
-- Update trigger for user_configs.updated_at
-- =============================================================
create or replace function update_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_user_configs_updated_at on public.user_configs;
create trigger trg_user_configs_updated_at
  before update on public.user_configs
  for each row execute procedure update_updated_at();

-- =============================================================
-- Auto-create empty user_config row when a new user signs up
-- =============================================================
create or replace function public.handle_new_user()
returns trigger as $$
begin
  insert into public.user_configs (user_id) values (new.id)
  on conflict (user_id) do nothing;
  return new;
end;
$$ language plpgsql security definer;

drop trigger if exists trg_on_auth_user_created on auth.users;
create trigger trg_on_auth_user_created
  after insert on auth.users
  for each row execute procedure public.handle_new_user();

-- =============================================================
-- TEAMS / WORKSPACES
-- Multi-tenant collaboration: owner creates team, invites members,
-- assigns roles with custom permissions. Backend enforces permission
-- checks (RLS here is minimal — member can read their own team row).
-- =============================================================
create table if not exists public.teams (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  slug text unique,
  owner_id uuid not null references auth.users(id) on delete cascade,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

create index if not exists idx_teams_owner on public.teams(owner_id);

-- =============================================================
-- team_roles — role definitions per team (admin, member, viewer, custom)
-- permissions is a jsonb flag bag:
--   { "admin": true, "manage_members": true, "manage_roles": true,
--     "manage_deals": true, "view_analytics": true }
-- =============================================================
create table if not exists public.team_roles (
  id uuid primary key default gen_random_uuid(),
  team_id uuid not null references public.teams(id) on delete cascade,
  name text not null,
  permissions jsonb not null default '{}'::jsonb,
  is_default boolean default false,
  is_system boolean default false,          -- Owner/Admin/Member cannot be deleted
  created_at timestamptz default now(),
  unique(team_id, name)
);

create index if not exists idx_team_roles_team on public.team_roles(team_id);

-- =============================================================
-- team_members — user ↔ team ↔ role
-- =============================================================
create table if not exists public.team_members (
  id uuid primary key default gen_random_uuid(),
  team_id uuid not null references public.teams(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role_id uuid references public.team_roles(id) on delete set null,
  added_by uuid references auth.users(id),
  added_at timestamptz default now(),
  unique(team_id, user_id)
);

create index if not exists idx_team_members_team on public.team_members(team_id);
create index if not exists idx_team_members_user on public.team_members(user_id);

-- =============================================================
-- team_invitations — pending invites by email with a one-time token
-- =============================================================
create table if not exists public.team_invitations (
  id uuid primary key default gen_random_uuid(),
  team_id uuid not null references public.teams(id) on delete cascade,
  email text not null,
  role_id uuid references public.team_roles(id) on delete set null,
  token text not null unique,
  invited_by uuid references auth.users(id),
  expires_at timestamptz not null default (now() + interval '14 days'),
  accepted_at timestamptz,
  accepted_by uuid references auth.users(id),
  revoked_at timestamptz,
  created_at timestamptz default now()
);

create index if not exists idx_team_invitations_team on public.team_invitations(team_id);
create index if not exists idx_team_invitations_email on public.team_invitations(lower(email));
create index if not exists idx_team_invitations_token on public.team_invitations(token);

-- =============================================================
-- RLS — minimal here; backend enforces fine-grained perms
-- Members can read their team, its roles, its member list, and
-- their own invitations. Writes go through backend (service role).
-- =============================================================
alter table public.teams enable row level security;
alter table public.team_roles enable row level security;
alter table public.team_members enable row level security;
alter table public.team_invitations enable row level security;

create policy "teams: member read"
  on public.teams for select
  using (
    owner_id = auth.uid()
    or exists (
      select 1 from public.team_members tm
      where tm.team_id = teams.id and tm.user_id = auth.uid()
    )
  );

create policy "teams: owner insert"
  on public.teams for insert
  with check (owner_id = auth.uid());

create policy "teams: owner update"
  on public.teams for update
  using (owner_id = auth.uid());

create policy "team_roles: member read"
  on public.team_roles for select
  using (
    exists (
      select 1 from public.team_members tm
      where tm.team_id = team_roles.team_id and tm.user_id = auth.uid()
    )
    or exists (
      select 1 from public.teams t
      where t.id = team_roles.team_id and t.owner_id = auth.uid()
    )
  );

create policy "team_members: same-team read"
  on public.team_members for select
  using (
    user_id = auth.uid()
    or exists (
      select 1 from public.team_members tm2
      where tm2.team_id = team_members.team_id and tm2.user_id = auth.uid()
    )
    or exists (
      select 1 from public.teams t
      where t.id = team_members.team_id and t.owner_id = auth.uid()
    )
  );

create policy "team_invitations: addressee read"
  on public.team_invitations for select
  using (
    lower(email) = lower(coalesce((auth.jwt() ->> 'email'), ''))
    or exists (
      select 1 from public.team_members tm
      where tm.team_id = team_invitations.team_id and tm.user_id = auth.uid()
    )
    or exists (
      select 1 from public.teams t
      where t.id = team_invitations.team_id and t.owner_id = auth.uid()
    )
  );

drop trigger if exists trg_teams_updated_at on public.teams;
create trigger trg_teams_updated_at
  before update on public.teams
  for each row execute procedure update_updated_at();

