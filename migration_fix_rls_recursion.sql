-- =============================================================
-- JP BrokerFlow — FIX: infinite recursion in team_members RLS
--
-- Paste into Supabase SQL Editor and Run:
--   https://supabase.com/dashboard/project/nctgbkqkfgbcndhrzxnu/sql/new
--
-- Problem: policy "team_members: same-team read" contains a
-- sub-select against team_members itself, which re-evaluates RLS
-- recursively (Postgres error 42P17). Also blocks any query that
-- joins team_members (including our Level-2 sub-team policies).
--
-- Fix: replace recursive/join-heavy policies with SECURITY DEFINER
-- helper functions that bypass RLS for the membership check, and
-- simpler direct policies for the base tables.
--
-- Safe to re-run (uses drop policy if exists / create or replace).
-- =============================================================

-- ---- SECURITY DEFINER helpers (bypass RLS for membership checks) ----
create or replace function public.is_workspace_member(ws uuid)
returns boolean
language sql
security definer
stable
set search_path = public
as $$
  select exists (
    select 1 from public.team_members tm
    where tm.team_id = ws and tm.user_id = auth.uid()
  )
  or exists (
    select 1 from public.teams t
    where t.id = ws and t.owner_id = auth.uid()
  );
$$;

grant execute on function public.is_workspace_member(uuid) to authenticated, anon;

create or replace function public.is_subteam_workspace_member(subteam uuid)
returns boolean
language sql
security definer
stable
set search_path = public
as $$
  select exists (
    select 1
    from public.workspace_teams wt
    where wt.id = subteam
      and public.is_workspace_member(wt.workspace_id)
  );
$$;

grant execute on function public.is_subteam_workspace_member(uuid) to authenticated, anon;

-- ---- Replace recursive Level-1 policies ----

drop policy if exists "teams: member read" on public.teams;
create policy "teams: member read"
  on public.teams for select
  using (
    owner_id = auth.uid()
    or public.is_workspace_member(id)
  );

drop policy if exists "team_roles: member read" on public.team_roles;
create policy "team_roles: member read"
  on public.team_roles for select
  using (public.is_workspace_member(team_id));

drop policy if exists "team_members: same-team read" on public.team_members;
create policy "team_members: same-team read"
  on public.team_members for select
  using (
    user_id = auth.uid()
    or public.is_workspace_member(team_id)
  );

drop policy if exists "team_invitations: addressee read" on public.team_invitations;
create policy "team_invitations: addressee read"
  on public.team_invitations for select
  using (
    lower(email) = lower(coalesce((auth.jwt() ->> 'email'), ''))
    or public.is_workspace_member(team_id)
  );

-- ---- Replace Level-2 policies (they also join team_members) ----

drop policy if exists "workspace_teams: workspace member read" on public.workspace_teams;
create policy "workspace_teams: workspace member read"
  on public.workspace_teams for select
  using (public.is_workspace_member(workspace_id));

drop policy if exists "workspace_team_roles: workspace member read" on public.workspace_team_roles;
create policy "workspace_team_roles: workspace member read"
  on public.workspace_team_roles for select
  using (public.is_subteam_workspace_member(team_id));

drop policy if exists "workspace_team_members: workspace member read" on public.workspace_team_members;
create policy "workspace_team_members: workspace member read"
  on public.workspace_team_members for select
  using (public.is_subteam_workspace_member(team_id));
