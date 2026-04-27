-- sl-map-site: per-listing user ratings/rejections.
-- Project-prefixed table name keeps this Supabase database easy to inspect when
-- multiple projects share the same free Supabase instance.

create table if not exists public.sl_map_site_listing_ratings (
  listing_id text primary key,
  rating integer check (rating between 0 and 10),
  rejected boolean not null default false,
  project_key text not null default 'sl-map-site',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists sl_map_site_listing_ratings_project_idx
  on public.sl_map_site_listing_ratings (project_key);

create index if not exists sl_map_site_listing_ratings_rating_idx
  on public.sl_map_site_listing_ratings (rating desc nulls last);

create index if not exists sl_map_site_listing_ratings_rejected_idx
  on public.sl_map_site_listing_ratings (rejected);

create or replace function public.sl_map_site_touch_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists sl_map_site_listing_ratings_touch_updated_at on public.sl_map_site_listing_ratings;
create trigger sl_map_site_listing_ratings_touch_updated_at
before update on public.sl_map_site_listing_ratings
for each row execute function public.sl_map_site_touch_updated_at();

alter table public.sl_map_site_listing_ratings enable row level security;

-- Browser clients read through our serverless API. The API writes with the
-- service-role key, so no public insert/update policy is needed.
drop policy if exists "sl_map_site_listing_ratings_service_role_all" on public.sl_map_site_listing_ratings;
create policy "sl_map_site_listing_ratings_service_role_all"
on public.sl_map_site_listing_ratings
for all
to service_role
using (true)
with check (true);
