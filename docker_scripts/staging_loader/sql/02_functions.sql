-- Shared cleaning rules (TODO 0-5). They return NULL instead of failing on a value they cannot read,
-- so the DQ checks can count "raw value present but parsed NULL" and quarantine those records.

-- '' / whitespace -> NULL (FSA empty strings)
CREATE OR REPLACE FUNCTION staging.blank_to_null(v TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT NULLIF(btrim(v), '')
$$;

-- Ofsted placeholders -> NULL: 'NULL', 'Not set', ''.
-- Not for the category of concern columns, where '' means "inspected, no concern" (handle them in place).
-- 'Not applicable', 'Does not apply', 'Not recorded', 'Not judged' are real values and are kept.
CREATE OR REPLACE FUNCTION staging.ofsted_null(v TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN btrim(v) IN ('', 'NULL', 'Not set') THEN NULL ELSE btrim(v) END
$$;

-- Integer from text; NULL if it is not a whole number
CREATE OR REPLACE FUNCTION staging.to_int(v TEXT) RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN v ~ '^\s*-?\d{1,9}\s*$' THEN btrim(v)::INTEGER END
$$;

-- Ofsted date dd/mm/yyyy -> DATE
CREATE OR REPLACE FUNCTION staging.parse_ofsted_date(v TEXT) RETURNS DATE
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN btrim(v) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(btrim(v), 'DD/MM/YYYY') END
$$;

-- FSA date YYYY-MM-DDTHH:MM:SS -> DATE; 1901 (not inspected yet) -> NULL
CREATE OR REPLACE FUNCTION staging.parse_fsa_date(v TEXT) RETURNS DATE
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN v ~ '^\d{4}-\d{2}-\d{2}' AND left(v, 4) <> '1901' THEN left(v, 10)::DATE END
$$;

-- Postcode: upper case, one space before the inward code (e.g. 'sp70pf' -> 'SP7 0PF').
-- A partial postcode (outward code only, e.g. 'BA1') or an unreadable value is returned upper case, unspaced.
CREATE OR REPLACE FUNCTION staging.normalise_postcode(v TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    WITH s AS (SELECT NULLIF(upper(regexp_replace(v, '\s', '', 'g')), '') AS p)
    SELECT CASE WHEN p ~ '^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$'
                THEN left(p, length(p) - 3) || ' ' || right(p, 3)
                ELSE p END
    FROM s
$$;

-- TRUE if the value is a full postcode (outward + inward code)
CREATE OR REPLACE FUNCTION staging.is_full_postcode(v TEXT) RETURNS BOOLEAN
LANGUAGE sql IMMUTABLE AS $$
    SELECT coalesce(staging.normalise_postcode(v) ~ '^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$', FALSE)
$$;

-- Name key to compare local authority names written differently by Ofsted and postcodes.io:
-- 'Kingston upon Hull, City of' = 'Kingston upon Hull', 'County Durham' = 'Durham',
-- 'Bournemouth, Christchurch & Poole' = '... and Poole', 'St. Helens' = 'St Helens'
CREATE OR REPLACE FUNCTION staging.name_key(v TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT NULLIF(btrim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(
               lower(v), '&', ' and ', 'g'),
               ',\s*(city|county) of\s*$|^county\s+', '', 'g'),
               '[.,''-]', ' ', 'g'),
               '\s+', ' ', 'g')), '')
$$;

-- Postcode sector: outward code + first digit of the inward code ('SP7 0PF' -> 'SP7 0'); NULL unless full
CREATE OR REPLACE FUNCTION staging.postcode_sector(v TEXT) RETURNS TEXT
LANGUAGE sql IMMUTABLE AS $$
    SELECT CASE WHEN staging.is_full_postcode(v)
                THEN left(staging.normalise_postcode(v), length(staging.normalise_postcode(v)) - 2) END
$$;
