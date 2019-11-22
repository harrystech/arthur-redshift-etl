CREATE OR REPLACE FUNCTION dw.f_sha256 (msg VARCHAR)
        returns varchar
STABLE
AS $$
        import hashlib
        return hashlib.sha256(msg).hexdigest()
$$ LANGUAGE plpythonu
;

/* TODO When loading functions, grant to the "default" group from the setup */
GRANT EXECUTE ON FUNCTION dw.f_sha256(msg varchar) TO GROUP analyst_ro
;
