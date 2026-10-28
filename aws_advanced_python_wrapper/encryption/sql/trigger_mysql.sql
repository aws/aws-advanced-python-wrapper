DELIMITER $$

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

-- HMAC-SHA256 built from SHA2(), which MySQL has no native HMAC for.
DROP FUNCTION IF EXISTS hmac_sha256$$
CREATE FUNCTION hmac_sha256(
    message VARBINARY(65535),
    key_data VARBINARY(64)
)
RETURNS VARBINARY(32)
DETERMINISTIC
NO SQL
BEGIN
    DECLARE block_size INT DEFAULT 64;  -- SHA-256 block size
    DECLARE key_padded VARBINARY(64);
    DECLARE ipad VARBINARY(64);
    DECLARE opad VARBINARY(64);
    DECLARE i INT DEFAULT 1;
    DECLARE key_byte INT;

    -- Reduce an over-long key to its hash, then zero-pad the key to the block size.
    IF LENGTH(key_data) > block_size THEN
        SET key_padded = CONCAT(UNHEX(SHA2(key_data, 256)), REPEAT(X'00', block_size - 32));
    ELSE
        SET key_padded = CONCAT(key_data, REPEAT(X'00', block_size - LENGTH(key_data)));
    END IF;

    -- ipad = key XOR 0x36 per byte, opad = key XOR 0x5C per byte, one raw byte at a time.
    SET ipad = X'';
    SET opad = X'';
    WHILE i <= block_size DO
        SET key_byte = ASCII(SUBSTRING(key_padded, i, 1));
        SET ipad = CONCAT(ipad, UNHEX(LPAD(HEX(key_byte ^ 0x36), 2, '0')));
        SET opad = CONCAT(opad, UNHEX(LPAD(HEX(key_byte ^ 0x5C), 2, '0')));
        SET i = i + 1;
    END WHILE;

    RETURN UNHEX(SHA2(CONCAT(opad, UNHEX(SHA2(CONCAT(ipad, message), 256))), 256));
END$$

-- Verifies that a stored value carries a valid HMAC for the given HMAC key.
DROP FUNCTION IF EXISTS verify_encrypted_data_hmac$$
CREATE FUNCTION verify_encrypted_data_hmac(
    data VARBINARY(65535),
    hmac_key VARBINARY(32)
)
RETURNS BOOLEAN
DETERMINISTIC
NO SQL
BEGIN
    -- Minimum payload: 32 (HMAC) + 4 (key id) + 1 (type) + 12 (IV) + 0 (ciphertext) + 16 (GCM tag) = 65.
    IF data IS NULL OR LENGTH(data) < 65 THEN
        RETURN FALSE;
    END IF;

    RETURN SUBSTRING(data, 1, 32) = hmac_sha256(SUBSTRING(data, 33), hmac_key);
END$$

-- Called from a BEFORE INSERT trigger (and by the BEFORE UPDATE procedure) to reject a value that is
-- not a valid kms_encryption payload. Replace SCHEMA_NAME with encryption_metadata_schema.
-- The COMMENT records which version of this SQL is installed. Check it with:
--   SELECT ROUTINE_COMMENT FROM information_schema.ROUTINES
--    WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'validate_encrypted_data_hmac_before_insert';
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_insert$$
CREATE PROCEDURE validate_encrypted_data_hmac_before_insert(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN column_value VARBINARY(65535)
)
COMMENT 'aws-advanced-python-wrapper kms_encryption trigger v1'
BEGIN
    DECLARE v_hmac_key VARBINARY(32);

    IF column_value IS NOT NULL THEN
        SELECT ks.hmac_key INTO v_hmac_key
        FROM SCHEMA_NAME.encryption_metadata em
        JOIN SCHEMA_NAME.key_storage ks ON em.key_id = ks.id
        WHERE em.table_name = p_table_name
          AND em.column_name = p_column_name
        LIMIT 1;

        IF v_hmac_key IS NULL THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'No HMAC key is configured for the encrypted column';
        END IF;

        IF NOT verify_encrypted_data_hmac(column_value, v_hmac_key) THEN
            SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Column does not carry a valid HMAC tag (plaintext or tampered value)';
        END IF;
    END IF;
END$$

-- Called from a BEFORE UPDATE trigger. An UPDATE that leaves the value as it was is not
-- re-checked; see "Updates after a key rotation" in the guide.
DROP PROCEDURE IF EXISTS validate_encrypted_data_hmac_before_update$$
CREATE PROCEDURE validate_encrypted_data_hmac_before_update(
    IN p_table_name VARCHAR(64),
    IN p_column_name VARCHAR(64),
    IN new_value VARBINARY(65535),
    IN old_value VARBINARY(65535)
)
BEGIN
    IF NOT (new_value <=> old_value) THEN
        CALL validate_encrypted_data_hmac_before_insert(p_table_name, p_column_name, new_value);
    END IF;
END$$

DELIMITER ;
