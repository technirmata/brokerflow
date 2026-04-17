-- =============================================================
-- JP BrokerFlow — Level-2 sub-team migration
-- Paste this into Supabase SQL Editor:
--   https://supabase.com/dashboard/project/nctgbkqkfgbcndhrzxnu/sql/new
-- Then click "Run".
--
-- Creates the sub-team layer inside each workspace:
--   workspace_teams        (sub-groups like "Acquisitions", "Asset Mgmt")
--   workspace_team_roles   (system + custom roles per sub-team)
--   workspace_team_members (user <-> sub-team <-> role)
--
-- Safe to re-run (uses `if not exists` / `drop trigger if exists`).
-- =============================================================

create table if not exists public.workspace_teams (
  id uuid primary key default gen_random_uuid(),
  workspace_id uuid not null references public.teams(id) on delete cascade,
  name text not null,
  description text,
  created_by uuid references auth.users(id),
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  unique(workspace_id, name)
);

create index if not exists idx_workspace_teams_workspace on public.workspace_teams(workspace_id);

create table if not exists public.workspace_team_roles (
  id uuid primary key default gen_random_uuid(),
  team_id uuid not null references public.workspace_teams(id) on delete cascade,
  name text not null,
  permissions jsonb not null default '{}'::jsonb,
  is_default boolean default false,
  is_system boolean default false,
  created_at timestamptz default now(),
  unique(team_id, name)
);

create index if not exists idx_workspace_team_roles_team on public.workspace_team_roles(team_id);

create table if not exists public.workspace_team_members (
  id uuid primary key default gen_random_uuid(),
  team_id uuid not null references public.workspace_teams(id) on delete cascade,
  user_id uuid not null references auth.users(id) on delete cascade,
  role_id uuid references public.workspace_team_roles(id) on delete set null,
  added_by uuid references auth.users(id),
  added_at timestamptz default now(),
  unique(team_id, user_id)
);

create index if not exists idx_workspace_team_members_team on public.workspace_team_members(team_id);
create index if not exists idx_workspace_team_members_user on public.workspace_team_members(user_id);

alter table public.workspace_teams enable row level security;
alter table public.workspace_team_roles enable row level security;
alter table public.workspace_team_members enable row level security;

drop policy if exists "workspace_teams: workspace member read" on public.workspace_teams;
create policy "workspace_teams: workspace member read"
  on public.workspace_teams for select
  using (
    exists (
      select 1 from public.team_members tm
      where tm.team_id = workspace_teams.workspace_id and tm.user_id = auth.uid()
    )
    or exists (
      select 1 from public.teams t
      where t.id = workspace_teams.workspace_id and t.owner_id = auth.uid()
    )
  );

drop policy if exists "workspace_team_roles: workspace member read" on public.workspace_team_roles;
create policy "workspace_team_roles: workspace member read"
  on public.workspace_team_roles for select
  using (
    exists (
      select 1 from public.workspace_teams wt
      join public.team_members tm on tm.team_id = wt.workspace_id
      where wt.id = workspace_team_roles.team_id and tm.user_id = auth.uid()
    )
    or exists (
      select 1 from public.workspace_teams wt
      join public.teams t on t.id = wt.workspace_id
      where wt.id = workspace_team_roles.team_id and t.owner_id = auth.uid()
    )
  );

drop policy if exists "workspace_team_members: workspace member read" on public.workspace_team_members;
create policy "workspace_team_members: workspace member read"
  on public.workspace_team_members for select
  using (
    exists (
      select 1 from public.workspace_teams wt
      join public.team_members tm on tm.team_id = wt.workspace_id
      where wt.id = workspace_team_members.team_id and tm.user_id = auth.uid()
    )
    or exists (
      select 1 from public.workspace_teams wt
      join public.teams t on t.id = wt.workspace_id
      where wt.id = workspace_team_members.team_id and t.owner_id = auth.uid()
    )
  );

drop trigger if exists trg_workspace_teams_updated_at on public.workspace_teams;
create trigger trg_workspace_teams_updated_at
  before update on public.workspace_teams
  for each row execute procedure update_updated_at();
