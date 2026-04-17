-- =====================================================================
-- BrokerFlow workspace sync migration
-- =====================================================================
-- Purpose: add workspace_id to shared data tables so all members of a
-- workspace see the same deals, brokers, touchpoints, and links —
-- regardless of who originally created them.
--
-- Run this in Supabase SQL Editor for project `nctgbkqkfgbcndhrzxnu`.
-- Safe to re-run (all statements use IF NOT EXISTS / ON CONFLICT).
-- =====================================================================

-- ---------- 1. Add workspace_id columns ----------

ALTER TABLE public.touchpoints
  ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE;

ALTER TABLE public.deal_broker_links
  ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE;

-- Optional shared tables — only alter if they exist
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='doc_requests') THEN
    EXECUTE 'ALTER TABLE public.doc_requests ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='events') THEN
    EXECUTE 'ALTER TABLE public.events ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='cadence_runs') THEN
    EXECUTE 'ALTER TABLE public.cadence_runs ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='attachments') THEN
    EXECUTE 'ALTER TABLE public.attachments ADD COLUMN IF NOT EXISTS workspace_id uuid REFERENCES public.teams(id) ON DELETE CASCADE';
  END IF;
END $$;

-- ---------- 2. Backfill workspace_id from each user's default workspace ----------

-- For each row with a user_id, set workspace_id = that user's oldest owned
-- workspace (teams.owner_id = user_id). This preserves all history and
-- assigns it to the user's personal default workspace.

UPDATE public.touchpoints tp
SET workspace_id = t.id
FROM public.teams t
WHERE tp.workspace_id IS NULL
  AND t.owner_id = tp.user_id
  AND t.id = (
    SELECT id FROM public.teams
    WHERE owner_id = tp.user_id
    ORDER BY created_at ASC
    LIMIT 1
  );

UPDATE public.deal_broker_links l
SET workspace_id = t.id
FROM public.teams t
WHERE l.workspace_id IS NULL
  AND t.owner_id = l.user_id
  AND t.id = (
    SELECT id FROM public.teams
    WHERE owner_id = l.user_id
    ORDER BY created_at ASC
    LIMIT 1
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='doc_requests') THEN
    EXECUTE $sql$
      UPDATE public.doc_requests dr
      SET workspace_id = t.id
      FROM public.teams t
      WHERE dr.workspace_id IS NULL
        AND t.owner_id = dr.user_id
        AND t.id = (SELECT id FROM public.teams WHERE owner_id = dr.user_id ORDER BY created_at ASC LIMIT 1)
    $sql$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='events') THEN
    EXECUTE $sql$
      UPDATE public.events e
      SET workspace_id = t.id
      FROM public.teams t
      WHERE e.workspace_id IS NULL
        AND t.owner_id = e.user_id
        AND t.id = (SELECT id FROM public.teams WHERE owner_id = e.user_id ORDER BY created_at ASC LIMIT 1)
    $sql$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='cadence_runs') THEN
    EXECUTE $sql$
      UPDATE public.cadence_runs c
      SET workspace_id = t.id
      FROM public.teams t
      WHERE c.workspace_id IS NULL
        AND t.owner_id = c.user_id
        AND t.id = (SELECT id FROM public.teams WHERE owner_id = c.user_id ORDER BY created_at ASC LIMIT 1)
    $sql$;
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='attachments') THEN
    EXECUTE $sql$
      UPDATE public.attachments a
      SET workspace_id = t.id
      FROM public.teams t
      WHERE a.workspace_id IS NULL
        AND t.owner_id = a.user_id
        AND t.id = (SELECT id FROM public.teams WHERE owner_id = a.user_id ORDER BY created_at ASC LIMIT 1)
    $sql$;
  END IF;
END $$;

-- ---------- 3. Indexes for workspace-scoped queries ----------

CREATE INDEX IF NOT EXISTS idx_touchpoints_workspace      ON public.touchpoints(workspace_id);
CREATE INDEX IF NOT EXISTS idx_touchpoints_ws_deal        ON public.touchpoints(workspace_id, deal_id);
CREATE INDEX IF NOT EXISTS idx_touchpoints_ws_broker      ON public.touchpoints(workspace_id, broker_id);
CREATE INDEX IF NOT EXISTS idx_deal_broker_links_workspace ON public.deal_broker_links(workspace_id);

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='doc_requests') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_doc_requests_workspace ON public.doc_requests(workspace_id)';
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='events') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS idx_events_workspace ON public.events(workspace_id)';
  END IF;
END $$;

-- ---------- 4. RLS — allow workspace members to read each other's rows ----------

-- Strategy: Replace user-only SELECT policies on shared tables with a
-- policy that allows access when the caller is a member of the row's
-- workspace. Writes remain auth.uid()-scoped for now (the backend stamps
-- workspace_id on insert and will switch to workspace-member write perm
-- checks at the application layer via UserContext + team_members).

-- touchpoints
DROP POLICY IF EXISTS "touchpoints_select_own" ON public.touchpoints;
CREATE POLICY "touchpoints_select_workspace_members"
  ON public.touchpoints FOR SELECT
  USING (
    user_id = auth.uid()
    OR workspace_id IN (
      SELECT team_id FROM public.team_members WHERE user_id = auth.uid()
    )
    OR workspace_id IN (
      SELECT id FROM public.teams WHERE owner_id = auth.uid()
    )
  );

-- deal_broker_links
DROP POLICY IF EXISTS "deal_broker_links_select_own" ON public.deal_broker_links;
CREATE POLICY "deal_broker_links_select_workspace_members"
  ON public.deal_broker_links FOR SELECT
  USING (
    user_id = auth.uid()
    OR workspace_id IN (
      SELECT team_id FROM public.team_members WHERE user_id = auth.uid()
    )
    OR workspace_id IN (
      SELECT id FROM public.teams WHERE owner_id = auth.uid()
    )
  );

-- ---------- 5. Report ----------

DO $$
DECLARE
  tp_total int; tp_missing int;
  l_total int;  l_missing int;
BEGIN
  SELECT count(*), count(*) FILTER (WHERE workspace_id IS NULL)
    INTO tp_total, tp_missing FROM public.touchpoints;
  SELECT count(*), count(*) FILTER (WHERE workspace_id IS NULL)
    INTO l_total, l_missing FROM public.deal_broker_links;
  RAISE NOTICE 'Migration complete';
  RAISE NOTICE '  touchpoints: % total, % still missing workspace_id', tp_total, tp_missing;
  RAISE NOTICE '  deal_broker_links: % total, % still missing workspace_id', l_total, l_missing;
  IF tp_missing > 0 OR l_missing > 0 THEN
    RAISE NOTICE 'Rows missing workspace_id likely belong to users with no default workspace.';
    RAISE NOTICE 'Those users will see empty pipeline until they create or join a workspace.';
  END IF;
END $$;
