--  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
--
--  Licensed under the Apache License, Version 2.0 (the "License").
--  You may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

-- Trigger SQL of the kms_encryption plugin. See "Enforce encryption in the database" in
-- docs/using-the-python-wrapper/using-plugins/UsingTheKmsEncryptionPlugin.md.

-- pgcrypto provides hmac(). If it is already installed in a schema other than public,
-- replace public in public.hmac below with that schema.
CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;

-- Replace 'encrypt' below if encryption_metadata_schema is set to something else.
-- A trigger runs with the search_path of the session that fires it. Pinning it to pg_catalog
-- stops a session from putting its own functions or operators, such as hmac() or <>, ahead of
-- the built-in ones to get a plaintext value past this check.
CREATE OR REPLACE FUNCTION enforce_encrypted_column() RETURNS trigger
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
  col_name  text := TG_ARGV[0];
  col_value bytea;
  hmac_key  bytea;
  unchanged boolean;
BEGIN
  -- An UPDATE that leaves the encrypted value as it was is not re-checked; see
  -- "Updates after a key rotation" in the guide.
  IF TG_OP = 'UPDATE' THEN
    EXECUTE format('SELECT ($1).%I IS NOT DISTINCT FROM ($2).%I', col_name, col_name)
      INTO unchanged USING NEW, OLD;
    IF unchanged THEN
      RETURN NEW;
    END IF;
  END IF;

  EXECUTE format('SELECT ($1).%I', col_name) INTO col_value USING NEW;
  IF col_value IS NULL THEN
    RETURN NEW;
  END IF;

  -- Read the column's current HMAC key from key_storage.
  SELECT ks.hmac_key INTO hmac_key
    FROM encrypt.encryption_metadata em
    JOIN encrypt.key_storage ks ON em.key_id = ks.id
   WHERE em.table_name = TG_TABLE_NAME AND em.column_name = col_name;

  IF hmac_key IS NULL THEN
    RAISE EXCEPTION 'No HMAC key is configured for %.%', TG_TABLE_NAME, col_name;
  END IF;

  IF length(col_value) < 65
     OR substring(col_value from 1 for 32) <> public.hmac(substring(col_value from 33), hmac_key, 'sha256'::text) THEN
    RAISE EXCEPTION 'Column %.% does not carry a valid HMAC tag (plaintext or tampered value)', TG_TABLE_NAME, col_name;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Records which version of this SQL is installed. Check it with:
--   SELECT obj_description('enforce_encrypted_column'::regproc, 'pg_proc');
COMMENT ON FUNCTION enforce_encrypted_column() IS 'aws-advanced-python-wrapper kms_encryption trigger v2';
