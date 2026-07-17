-- Synthetic source namespaces inspired by core-banking module boundaries.
-- They are not copies of, or claims about, the bank's real Oracle schemas.

CREATE SCHEMA IF NOT EXISTS mms;
CREATE SCHEMA IF NOT EXISTS krd;
CREATE SCHEMA IF NOT EXISTS prm;

COMMENT ON SCHEMA mms IS
    'Synthetic customer-oriented source schema for the local CDC pilot; not the real bank MMS model.';
COMMENT ON SCHEMA krd IS
    'Synthetic lending source schema for the local CDC pilot; not the real bank KRD model.';
COMMENT ON SCHEMA prm IS
    'Synthetic reference and parameter source schema for the local CDC pilot; not the real bank PRM model.';
