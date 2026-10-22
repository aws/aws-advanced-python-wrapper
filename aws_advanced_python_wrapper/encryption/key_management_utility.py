#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, List, Optional, cast

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.buffers import wipe
from aws_advanced_python_wrapper.encryption.connection_source import \
    CallerConnectionSource
from aws_advanced_python_wrapper.encryption.data_key_cache import DataKeyCache
from aws_advanced_python_wrapper.encryption.key_manager import KeyManager
from aws_advanced_python_wrapper.encryption.metadata_manager import \
    MetadataManager
from aws_advanced_python_wrapper.encryption.models import KeyMetadata, mask_arn
from aws_advanced_python_wrapper.encryption.sql_dialect import (SqlDialect,
                                                                execute,
                                                                fetch_rows)
from aws_advanced_python_wrapper.errors import KeyManagementError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import WrapperProperties

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.encryption.config import EncryptionConfig

logger = Logger(__name__)


class KeyManagementUtility:
    """Administrative operations for the kms_encryption plugin: create master
    keys, turn encryption on or off for a column, and rotate data keys.

    Nothing here runs during normal query execution. Call it from a migration
    or setup script on a plain psycopg / mysql-connector connection (or an
    ``AwsWrapperConnection``) that you own::

        import boto3
        from aws_advanced_python_wrapper.encryption.config import EncryptionConfig
        from aws_advanced_python_wrapper.encryption.key_management_utility import KeyManagementUtility
        from aws_advanced_python_wrapper.encryption.key_manager import KeyManager

        config = EncryptionConfig.from_props(
            {"encryption_kms_region": "us-east-1"}, require_allowed_master_keys=False)
        # create_kms_client turns off botocore's own retries, which would
        # otherwise multiply with the utility's.
        kms = KeyManager.create_kms_client(boto3.Session(), config)
        utility = KeyManagementUtility(conn, kms, config)
        arn = utility.create_master_key("users.ssn encryption")
        utility.initialize_encryption_for_column("users", "ssn", arn)
        conn.commit()

    The connection is never closed, committed, or rolled back here. Each
    operation's statements run inside your transaction, so a failed setup can
    be rolled back as a unit, and nothing is stored until you commit. A KMS
    master key created before a rollback is not deleted.

    Unlike the plugin, the utility works without
    ``encryption_allowed_master_key_arns``. When the allow-list is configured,
    it gates every data-key operation here too (not :meth:`create_master_key`
    or :meth:`is_master_key_valid`). Add every master key a column uses to the
    application's allow-list, or the plugin refuses that column.

    After a change here, running applications pick it up at their next
    metadata refresh, or once their cached metadata expires if that comes
    first (``encryption_metadata_cache_refresh_interval_sec``,
    ``encryption_metadata_cache_expiration_sec``).

    This does not create the database-side HMAC-validation trigger, which is
    required on every encrypted column.
    """

    ALIAS_PREFIX = "alias/python-kms-encryption-"

    def __init__(self, connection: Any, kms_client: Any, config: EncryptionConfig,
                 *, engine: Optional[str] = None):
        """
        :param engine: ``"postgresql"`` or ``"mysql"``. Inferred from
            ``connection`` when omitted.
        """
        if connection is None or kms_client is None or config is None:
            raise ValueError(Messages.get("KeyManagementUtility.MissingArgument"))
        self._sql = SqlDialect.for_name(engine) if engine else SqlDialect.for_connection(connection)
        self._connections = CallerConnectionSource(connection)
        self._kms = kms_client
        self._config = config
        self._schema = config.metadata_schema  # interpolated into every statement below
        # The utility never decrypts a data key, so there is nothing to cache.
        self._key_manager = KeyManager(kms_client, self._connections, self._sql, config,
                                       DataKeyCache(1, 1.0, enabled=False))
        # Never started, so every lookup reads the database through the
        # caller's connection and sees this transaction's own changes.
        self._metadata = MetadataManager(self._connections, self._sql, config)

    def create_master_key(self, description: str, *, policy: Optional[str] = None,
                          create_alias: bool = True) -> str:
        """Create a symmetric KMS master key and return its ARN. A failure to
        create the alias is logged, not raised: the key works by ARN."""
        if description is None:
            raise ValueError(Messages.get_formatted("KeyManagementUtility.RequiredArgument", "description"))
        arn = self._key_manager.create_master_key(description, policy=policy)
        if create_alias:
            alias = f"{self.ALIAS_PREFIX}{int(time.time() * 1000)}"
            try:
                self._kms.create_alias(AliasName=alias, TargetKeyId=arn)
            except Exception as e:
                logger.warning("KeyManagementUtility.CreateAliasFailed", mask_arn(arn), e)
        logger.info("KeyManagementUtility.CreatedMasterKey", mask_arn(arn))
        return arn

    def initialize_encryption_for_column(self, table_name: str, column_name: str, master_key_arn: str,
                                         algorithm: str = EncryptionAlgorithm.DEFAULT) -> int:
        """Set up encryption for a column that is not encrypted yet.

        :return: the ``key_storage.id`` of the new data key.
        :raises KeyManagementError: if the column is already encrypted or the
            setup fails.
        """
        self._require(table_name=table_name, column_name=column_name, master_key_arn=master_key_arn)
        if self._metadata.is_column_encrypted(table_name, column_name):
            raise KeyManagementError(
                Messages.get_formatted("KeyManagementUtility.AlreadyEncrypted", table_name, column_name),
                operation="INITIALIZE_COLUMN")
        return self.generate_and_store_data_key(table_name, column_name, master_key_arn, algorithm)

    def generate_and_store_data_key(self, table_name: str, column_name: str, master_key_arn: str,
                                    algorithm: str = EncryptionAlgorithm.DEFAULT) -> int:
        """Generate a data key for a column and record it, replacing any
        configuration the column already has (an upsert, so a setup script can
        be re-run).

        :return: the ``key_storage.id`` of the new data key.
        """
        self._require(table_name=table_name, column_name=column_name, master_key_arn=master_key_arn)
        algorithm = algorithm or EncryptionAlgorithm.DEFAULT
        if not EncryptionAlgorithm.is_supported(algorithm):
            raise EncryptionAlgorithm.unsupported(algorithm)

        key_storage_id = self._store_new_data_key(table_name, column_name, master_key_arn, algorithm)
        sql = (f"INSERT INTO {self._schema}.encryption_metadata "
               "(table_name, column_name, encryption_algorithm, key_id) VALUES (%s, %s, %s, %s) "
               + self._sql.upsert_clause(("table_name", "column_name"), ("encryption_algorithm", "key_id"),
                                         extra_assignments=("updated_at = CURRENT_TIMESTAMP",)))
        self._run("STORE_ENCRYPTION_METADATA", sql, (table_name, column_name, algorithm, key_storage_id))
        logger.info("KeyManagementUtility.StoredDataKey", table_name, column_name, key_storage_id)
        return key_storage_id

    def rotate_data_key(self, table_name: str, column_name: str, new_master_key_arn: Optional[str] = None) -> int:
        """Give an encrypted column a new data key. New writes use it; values
        written with the previous key stay readable, because each payload
        records its ``key_storage.id`` and the old row is kept.

        Omitting ``new_master_key_arn`` keeps the master key recorded in
        ``key_storage``. That is only allowed with an allow-list configured to
        check it against, since otherwise a row repointed at another key would
        be trusted.

        :return: the ``key_storage.id`` of the new data key.
        """
        self._require(table_name=table_name, column_name=column_name)
        if new_master_key_arn is None and not self._config.restricts_master_keys:
            raise ValueError(Messages.get_formatted(
                "KeyManagementUtility.RotateNeedsMasterKey", WrapperProperties.ENCRYPTION_ALLOWED_MASTER_KEY_ARNS.name))

        current = self._metadata.column_config(table_name, column_name)
        if current is None:
            raise KeyManagementError(
                Messages.get_formatted("KeyManagementUtility.NotEncrypted", table_name, column_name),
                operation="ROTATE_DATA_KEY")
        master_key_arn = new_master_key_arn or (current.key_metadata.master_key_arn if current.key_metadata else None)
        if not master_key_arn:
            raise KeyManagementError(
                Messages.get_formatted("KeyManagementUtility.NoCurrentMasterKey", table_name, column_name),
                operation="ROTATE_DATA_KEY")

        key_storage_id = self._store_new_data_key(table_name, column_name, master_key_arn, current.algorithm)
        sql = (f"UPDATE {self._schema}.encryption_metadata SET key_id = %s, updated_at = CURRENT_TIMESTAMP "
               f"WHERE table_name {self._sql.equals} %s AND column_name {self._sql.equals} %s")
        if self._run("UPDATE_ENCRYPTION_METADATA", sql, (key_storage_id, table_name, column_name)) == 0:
            raise KeyManagementError(
                Messages.get_formatted("KeyManagementUtility.NotEncrypted", table_name, column_name),
                operation="ROTATE_DATA_KEY")
        logger.info("KeyManagementUtility.RotatedDataKey", table_name, column_name, current.key_id, key_storage_id)
        return key_storage_id

    def remove_encryption_for_column(self, table_name: str, column_name: str) -> bool:
        """Stop encrypting a column by deleting its ``encryption_metadata``
        row. The key stays in ``key_storage`` so values already written can
        still be decrypted.

        :return: ``True`` when a configuration row was removed.
        """
        self._require(table_name=table_name, column_name=column_name)
        sql = (f"DELETE FROM {self._schema}.encryption_metadata "
               f"WHERE table_name {self._sql.equals} %s AND column_name {self._sql.equals} %s")
        removed = self._run("DELETE_ENCRYPTION_METADATA", sql, (table_name, column_name)) > 0
        if not removed:
            logger.warning("KeyManagementUtility.NothingToRemove", table_name, column_name)
        return removed

    def columns_using_key(self, key_storage_id: int) -> List[str]:
        """The ``"table.column"`` names that use a ``key_storage.id``, to see
        what rotating or retiring that key would affect."""
        sql = (f"SELECT table_name, column_name FROM {self._schema}.encryption_metadata "
               f"WHERE key_id {self._sql.equals} %s ORDER BY table_name, column_name")
        try:
            with self._connections.connection() as conn:
                rows = fetch_rows(conn, sql, (key_storage_id,))
        except Exception as e:
            raise KeyManagementError(Messages.get_formatted("KeyManagementUtility.StatementFailed",
                                                            "SELECT_COLUMNS_USING_KEY", e),
                                     operation="SELECT_COLUMNS_USING_KEY") from e
        return [f"{r['table_name']}.{r['column_name']}" for r in rows]

    def is_master_key_valid(self, master_key_arn: str) -> bool:
        """``True`` when the master key exists, is enabled, and can encrypt
        and decrypt. ``False`` also covers "could not check"; see
        :meth:`KeyManager.is_master_key_valid`."""
        self._require(master_key_arn=master_key_arn)
        return self._key_manager.is_master_key_valid(master_key_arn)

    # ---- internals ----

    def _store_new_data_key(self, table_name: str, column_name: str, master_key_arn: str,
                            algorithm: str) -> int:
        """Generate a data key, store it, and return its ``key_storage.id``."""
        generated = self._key_manager.generate_data_key(master_key_arn, algorithm)
        try:
            # Nothing here encrypts with the plaintext key; only its
            # KMS-encrypted form is stored.
            stored = self._key_manager.store_key_metadata(KeyMetadata(
                key_name=f"key-{table_name}-{column_name}-{int(time.time() * 1000)}",
                master_key_arn=master_key_arn,
                encrypted_data_key=generated.encrypted_data_key,
                hmac_key=generated.hmac_key,
                key_spec=generated.key_spec,
            ))
            return cast("int", stored.id)
        finally:
            wipe(generated.plaintext)

    def _run(self, operation: str, sql: str, params: Any) -> int:
        try:
            with self._connections.connection() as conn:
                return execute(conn, sql, params)
        except Exception as e:
            raise KeyManagementError(Messages.get_formatted("KeyManagementUtility.StatementFailed", operation, e),
                                     operation=operation) from e

    @staticmethod
    def _require(**arguments: Any) -> None:
        for name, value in arguments.items():
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(Messages.get_formatted("KeyManagementUtility.RequiredArgument", name))
