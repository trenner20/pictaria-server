CREATE OR REPLACE FUNCTION pictaria_json_value(value text, path text)
RETURNS jsonb LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT value::jsonb #> string_to_array(substr(path, 3), '.')
$$;

CREATE OR REPLACE FUNCTION json_extract(value text, path text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT pictaria_json_value(value, path) #>> '{}'
$$;

CREATE OR REPLACE FUNCTION pictaria_json_number(value text, path text)
RETURNS bigint LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT CASE jsonb_typeof(item)
    WHEN 'boolean' THEN CASE WHEN item = 'true'::jsonb THEN 1 ELSE 0 END
    WHEN 'number' THEN (item #>> '{}')::numeric::bigint
    END
  FROM pictaria_json_value(value, path) item
$$;

CREATE OR REPLACE FUNCTION json_type(value text, path text)
RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT CASE jsonb_typeof(item)
    WHEN 'boolean' THEN item #>> '{}'
    WHEN 'number' THEN CASE WHEN (item #>> '{}') ~ '^-?[0-9]+$' THEN 'integer' ELSE 'real' END
    WHEN 'string' THEN 'text'
    ELSE jsonb_typeof(item) END
  FROM pictaria_json_value(value, path) item
$$;

CREATE OR REPLACE FUNCTION json_valid(value text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
BEGIN
  PERFORM value::jsonb;
  RETURN true;
EXCEPTION WHEN data_exception THEN
  RETURN false;
END
$$;

CREATE OR REPLACE FUNCTION json_set(value text, path text, replacement bigint)
RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path FROM CURRENT AS $$
  SELECT jsonb_set(value::jsonb, string_to_array(substr(path, 3), '.'), to_jsonb(replacement))::text
$$;

CREATE OR REPLACE FUNCTION julianday(value text)
RETURNS double precision LANGUAGE plpgsql IMMUTABLE STRICT
SET TimeZone = 'UTC' SET DateStyle = 'ISO, YMD' AS $$
BEGIN
  RETURN extract(epoch FROM (value::timestamptz)) / 86400 + 2440587.5;
EXCEPTION WHEN data_exception THEN
  RETURN NULL;
END
$$;

CREATE OR REPLACE FUNCTION strftime(format text, value text)
RETURNS text LANGUAGE plpgsql IMMUTABLE STRICT SET DateStyle = 'ISO, YMD' AS $$
BEGIN
  IF format <> '%Y-%m-%d' OR value !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
    RETURN NULL;
  END IF;
  RETURN to_char(value::date, 'YYYY-MM-DD');
EXCEPTION WHEN data_exception THEN
  RETURN NULL;
END
$$;

