# BrokerFlow — First-Run Setup

BrokerFlow is a multi-tenant deal-tracking dashboard for real estate
acquisitions. Each user signs up with email + password (or Google, or as a
guest), pastes their own ClickUp / SMTP / Twilio / Anthropic keys in-app, and
BrokerFlow stores per-user config in Supabase with row-level security.

This document walks you through a one-time setup so the backend knows which
Supabase project to use.

---

## 1. Create a Supabase project

1. Go to <https://supabase.com> and create a free project.
2. Pick any region close to you — 500 MB and 50k monthly active users are
   included free.
3. Wait ~2 minutes for the project to provision.

## 2. Run the schema

1. Open **SQL Editor** (left sidebar).
2. Paste the contents of `supabase_schema.sql` and click **Run**.
3. Verify four tables appeared under **Database → Tables**:
   `user_configs`, `touchpoints`, `deal_broker_links`, `email_threads_cache`.
4. Confirm RLS is enabled on each (lock icon visible).

## 3. Grab your credentials

From **Project Settings → API**:

| Key | Where to find it |
|-----|------------------|
| `SUPABASE_URL` | Project URL, e.g. `https://abcdef.supabase.co` |
| `SUPABASE_ANON_KEY` | Project API keys → `anon` `public` |
| `SUPABASE_JWT_SECRET` | JWT Settings → JWT Secret |

The `anon` key is safe to expose in the browser — RLS is what actually
enforces isolation. The `JWT_SECRET` is server-only.

## 4. Set them on Render

1. Open your Render service → **Environment**.
2. Add three env vars:
   ```
   SUPABASE_URL=https://abcdef.supabase.co
   SUPABASE_ANON_KEY=eyJhbGci...
   SUPABASE_JWT_SECRET=<your-jwt-secret>
   ```
3. Save — Render redeploys automatically.

That's it for the server side.

## 5. (Optional) Enable Google sign-in

If you want the "Continue with Google" button to work:

1. Supabase → **Authentication → Providers → Google → Enable**.
2. Follow Supabase's built-in instructions to create a Google OAuth client.
3. Set the authorized redirect URL shown by Supabase.
4. Paste the Google client ID + secret back into Supabase and save.

Email/password and anonymous (guest) login work out of the box with no extra
config.

## 6. (Optional) Email-confirmation vs instant signup

By default, Supabase sends a confirmation email before a new account can log
in. To skip that in development:

- Supabase → **Authentication → Providers → Email → Confirm email = off**.

## 7. First user walkthrough

1. Open the app — you'll land on the sign-in page.
2. Create an account (or continue as guest).
3. Paste your ClickUp personal token, pick a workspace + space, and let
   BrokerFlow create a `BrokerFlow` folder with 5 lists:
   `Active Deals`, `Broker Directory`, `Follow-ups Queue`,
   `Message Templates`, `Touchpoints Log`.
4. Add SMTP creds if you want to send email follow-ups (Gmail users: create
   an [app password](https://myaccount.google.com/apppasswords)).
5. Add Twilio creds if you want SMS.
6. Add an Anthropic API key if you want Claude-drafted follow-ups.
7. Each of these is optional — you can skip the wizard and fill them in later
   under **Settings**.

## 8. Ongoing use

- Every user sees only their own ClickUp data + touchpoints (RLS).
- Touchpoints (every email/SMS/call/note) are logged in Supabase and show up
  in broker and deal timelines.
- Keys are masked in the Settings UI after save (`••••••abc123`). Re-saving
  a masked value is a no-op — saves don't wipe keys.

## Troubleshooting

- **"Server not configured"** splash screen → the three env vars aren't set
  on Render. Re-check spelling and redeploy.
- **ClickUp lists not set up** error on `/api/v2/deals` → user hasn't
  completed Step 1 of the wizard. Reopen the wizard from Settings.
- **SMTP auth failed** → for Gmail, you must use an app password, not your
  regular Google password. For Outlook, ensure IMAP/SMTP is enabled.
- **Twilio `21608` error on send** → number isn't verified. Either upgrade
  the account or add your target phone under Verified Caller IDs.

## Rollback

The legacy single-tenant dashboard (driven by the server-side `CLICKUP_TOKEN`
env var) is still reachable at `/legacy` for backward compatibility.
