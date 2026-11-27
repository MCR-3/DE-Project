# Schools are matched to FSA authorities by the district of their postcode

Every KPI compares schools and food businesses within a local authority, but the two sources do not use the same authorities: Ofsted names the education authority (a county such as Kent in two-tier areas), the FSA the food-hygiene authority (a district such as Sevenoaks). Matching by name left about 42 % of schools without an authority. Staging therefore locates each school: its postcode is looked up on postcodes.io, whose ONS district code (`codes.admin_district`, e.g. `E07000111`) is mapped to the FSA `la_code` by a reviewed seed, `seeds/district_la_map.csv` (296 English districts: 265 same name, 22 spelling only, 9 by hand, column `match_method`). A terminated postcode has coordinates but no district, so the district of the nearest live postcode within 2 km is used (reverse geocoding), and accepted only if it belongs to the school's Ofsted authority (the district itself for a unitary authority, or its county). `la_code` is a `VARCHAR`, like the warehouse's `Dim_Geography.Local_Authority_Code`.

## Considered Options

- **Name mapping table (Ofsted name -> FSA name)**: fixes the spelling differences (14 authorities) but not the 21 counties, which have no single FSA authority. Rejected.
- **Roll FSA districts up to counties**: would match Ofsted's unit, but loses the district detail the FSA data has and needs its own district -> county table anyway. Rejected; the county is kept (`admin_county_*`) for a county roll-up in the warehouse.
- **Match names at run time instead of a seed**: adapts to new districts automatically, but a near-miss name can silently attach a school to the wrong authority. Rejected: the seed is reviewed, and a district missing from it fails the run.

## Consequences

- `la_code` is where the school is, not who maintains it: 37 boundary schools are located in a district other than their Ofsted authority (`stg_school.local_authority_name` keeps Ofsted's).
- postcodes.io is needed for schools as well as for the 1-mile analysis; its lookups are landed like any source (`docker_scripts/postcode_scraper`, cached 90 days) and kept per postcode in `staging.stg_postcode`.
- A new ONS district code (a local government reorganisation) fails the geocoding step until it is added to the seed.
- The two FSA port health authorities (Hull and Goole Port, River Tees) have no district and never get a school.
