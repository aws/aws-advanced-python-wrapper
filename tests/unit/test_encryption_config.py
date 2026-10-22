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

import pytest

from aws_advanced_python_wrapper.encryption.algorithm import \
    EncryptionAlgorithm
from aws_advanced_python_wrapper.encryption.config import (
    EncryptionConfig, validate_schema_name)
from aws_advanced_python_wrapper.encryption.models import (
    ColumnEncryptionConfig, KeyMetadata, mask_arn)
from aws_advanced_python_wrapper.errors import (EncryptionConfigError,
                                                InterfaceError)
from aws_advanced_python_wrapper.utils.properties import Properties

ARN = "arn:aws:kms:us-east-1:123456789012:key/1234abcd-12ab-34cd-56ef-1234567890ab"


@pytest.fixture(autouse=True)
def no_region_env(monkeypatch):
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)


def props(**kwargs) -> Properties:
    base = {"encryption_kms_region": "us-east-1", "encryption_allowed_master_key_arns": ARN}
    base.update(kwargs)
    return Properties({k: v for k, v in base.items() if v is not None})


class TestFromProps:
    def test_defaults_match_the_shared_spec(self):
        config = EncryptionConfig.from_props(props())
        assert config.kms_region == "us-east-1"
        assert config.kms_endpoint is None
        assert config.metadata_schema == "encrypt"
        assert config.metadata_cache_enabled is True
        assert config.metadata_cache_expiration_sec == 3600.0
        assert config.metadata_cache_refresh_interval_sec == 300.0
        assert config.data_key_cache_enabled is True
        assert config.data_key_cache_max_size == 1000
        assert config.data_key_cache_expiration_sec == 300.0
        assert config.key_management_max_retries == 3
        assert config.key_management_retry_backoff_base_sec == 0.1
        assert config.audit_logging_enabled is False
        assert config.return_unverified_data is False

    def test_string_values_are_parsed(self):
        config = EncryptionConfig.from_props(props(
            encryption_metadata_cache_enabled="false",
            encryption_metadata_cache_refresh_interval_sec="0",
            encryption_data_key_cache_max_size="5",
            encryption_key_management_retry_backoff_base_sec="0.25",
        ))
        assert config.metadata_cache_enabled is False
        assert config.metadata_cache_refresh_interval_sec == 0.0
        assert config.background_refresh_enabled is False
        assert config.data_key_cache_max_size == 5
        assert config.key_management_retry_backoff_base_sec == 0.25

    def test_region_falls_back_to_aws_region_then_default_region(self, monkeypatch):
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        assert EncryptionConfig.from_props(props(encryption_kms_region=None)).kms_region == "eu-west-1"
        monkeypatch.setenv("AWS_REGION", "ap-south-1")
        assert EncryptionConfig.from_props(props(encryption_kms_region=None)).kms_region == "ap-south-1"

    @pytest.mark.parametrize("env", ["AWS_REGION", "AWS_DEFAULT_REGION"])
    def test_region_property_beats_environment(self, monkeypatch, env):
        # On Lambda, ECS, or EC2 AWS_REGION is usually set; an explicit
        # encryption_kms_region must still win.
        monkeypatch.setenv(env, "eu-west-1")
        assert EncryptionConfig.from_props(props()).kms_region == "us-east-1"

    def test_blank_region_property_falls_back_to_the_environment(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        assert EncryptionConfig.from_props(props(encryption_kms_region="   ")).kms_region == "eu-west-1"

    @pytest.mark.parametrize("name", [
        "encryption_metadata_cache_expiration_sec",
        "encryption_metadata_cache_refresh_interval_sec",
        "encryption_data_key_cache_expiration_sec",
        "encryption_key_management_retry_backoff_base_sec",
    ])
    def test_infinite_durations_are_refused(self, name):
        # Event.wait() rejects an infinite timeout, which would kill the
        # background threads.
        with pytest.raises(EncryptionConfigError, match=name):
            EncryptionConfig.from_props(props(**{name: "inf"}))

    def test_blank_aws_region_falls_back_to_aws_default_region(self, monkeypatch):
        monkeypatch.setenv("AWS_REGION", "  ")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
        assert EncryptionConfig.from_props(props(encryption_kms_region=None)).kms_region == "eu-west-1"

    def test_region_is_stripped(self):
        assert EncryptionConfig.from_props(props(encryption_kms_region=" us-east-1 ")).kms_region == "us-east-1"

    def test_missing_region_is_rejected(self):
        with pytest.raises(EncryptionConfigError, match="region"):
            EncryptionConfig.from_props(props(encryption_kms_region=None))

    def test_allow_list_is_required_by_default(self):
        with pytest.raises(EncryptionConfigError, match="encryption_allowed_master_key_arns"):
            EncryptionConfig.from_props(props(encryption_allowed_master_key_arns=None))
        with pytest.raises(EncryptionConfigError):
            EncryptionConfig.from_props(props(encryption_allowed_master_key_arns=" , "))

    def test_allow_list_can_be_waived_for_admin_use(self):
        config = EncryptionConfig.from_props(
            props(encryption_allowed_master_key_arns=None), require_allowed_master_keys=False)
        assert not config.restricts_master_keys
        assert config.is_master_key_allowed(ARN)
        assert config.is_master_key_allowed(None)

    def test_non_numeric_value_is_a_config_error_naming_the_property(self):
        with pytest.raises(EncryptionConfigError, match="encryption_data_key_cache_max_size"):
            EncryptionConfig.from_props(props(encryption_data_key_cache_max_size="lots"))

    @pytest.mark.parametrize("name, value", [
        ("encryption_metadata_cache_expiration_sec", "0"),
        ("encryption_metadata_cache_refresh_interval_sec", "-1"),
        ("encryption_data_key_cache_max_size", "0"),
        ("encryption_data_key_cache_expiration_sec", "-5"),
        ("encryption_key_management_max_retries", "-1"),
        ("encryption_key_management_retry_backoff_base_sec", "0"),
    ])
    def test_out_of_range_values_name_the_property(self, name, value):
        with pytest.raises(EncryptionConfigError, match=name) as err:
            EncryptionConfig.from_props(props(**{name: value}))
        assert ">" in str(err.value).partition("must be")[2]

    def test_config_error_is_an_interface_error(self):
        with pytest.raises(InterfaceError):
            EncryptionConfig.from_props(props(encryption_kms_region=None))


class TestAllowList:
    def test_string_list_is_split_trimmed_and_deduplicated(self):
        other = ARN.replace("1234abcd", "9999ffff")
        config = EncryptionConfig.from_props(props(encryption_allowed_master_key_arns=f" {ARN} ,{other},{ARN},"))
        assert config.allowed_master_key_arns == frozenset({ARN, other})

    def test_python_list_is_accepted(self):
        config = EncryptionConfig.from_props(props(encryption_allowed_master_key_arns=[ARN, " "]))
        assert config.allowed_master_key_arns == frozenset({ARN})

    def test_match_is_exact(self):
        config = EncryptionConfig.from_props(props())
        assert config.is_master_key_allowed(ARN)
        assert not config.is_master_key_allowed(ARN.upper())
        assert not config.is_master_key_allowed(ARN + " ")
        assert not config.is_master_key_allowed("alias/whatever")
        assert not config.is_master_key_allowed(None)

    def test_direct_construction_normalizes_too(self):
        config = EncryptionConfig(kms_region="us-east-1", allowed_master_key_arns=f"{ARN}, ")  # type: ignore[arg-type]
        assert config.allowed_master_key_arns == frozenset({ARN})


class TestSchemaName:
    @pytest.mark.parametrize("name", ["encrypt", "_x", "App_Encrypt2"])
    def test_plain_identifiers_are_accepted(self, name):
        assert validate_schema_name(name) == name

    @pytest.mark.parametrize("name", ["", " ", "1abc", "a-b", "a b", "a;drop", "a.b", '"q"', "a/*c*/", None, 5])
    def test_anything_else_is_rejected(self, name):
        with pytest.raises(EncryptionConfigError):
            validate_schema_name(name)

    def test_props_schema_is_validated(self):
        with pytest.raises(EncryptionConfigError):
            EncryptionConfig.from_props(props(encryption_metadata_schema="x; DROP TABLE y"))


class TestModels:
    def test_key_metadata_repr_redacts_key_material(self):
        km = KeyMetadata(id=1, key_id="uuid", master_key_arn=ARN, encrypted_data_key="SECRET-EDK",
                         hmac_key=b"SECRET-HMAC-KEY-BYTES")
        for text in (repr(km), str(km), repr(ColumnEncryptionConfig("t", "c", key_metadata=km))):
            assert "SECRET" not in text
            assert "123456789012" not in text
            assert "[REDACTED]" in text

    def test_mask_arn(self):
        assert mask_arn(ARN) == "arn:aws:kms:***:***:key/1234***90ab"
        assert mask_arn("short/abc") == "arn:aws:kms:***:***:key/***"
        assert mask_arn("no-slash") == "arn:aws:kms:***:***:key/***"
        assert mask_arn(None) is None

    def test_usability(self):
        good = KeyMetadata(master_key_arn=ARN, encrypted_data_key="e", hmac_key=b"h")
        assert good.is_usable
        assert not KeyMetadata(master_key_arn=" ", encrypted_data_key="e", hmac_key=b"h").is_usable
        assert not KeyMetadata(master_key_arn=ARN, encrypted_data_key=None, hmac_key=b"h").is_usable
        assert not KeyMetadata(master_key_arn=ARN, encrypted_data_key="e", hmac_key=b"").is_usable
        assert ColumnEncryptionConfig("t", "c", key_metadata=good).is_usable
        assert not ColumnEncryptionConfig("t", "c", key_metadata=None).is_usable
        assert not ColumnEncryptionConfig("t", "c", algorithm="DES", key_metadata=good).is_usable

    def test_from_row_normalizes_binary_and_prefix(self):
        row = {"ks_id": "7", "ks_key_id": "u", "ks_hmac_key": memoryview(b"hk"), "ks_key_spec": None}
        km = KeyMetadata.from_row(row, "ks_")
        assert km.id == 7 and km.key_id == "u" and km.hmac_key == b"hk" and km.key_spec == "AES_256"


def test_data_key_spec_matches_algorithm_key_length():
    assert EncryptionAlgorithm.data_key_spec(EncryptionAlgorithm.AES_256_GCM) == "AES_256"
    assert EncryptionAlgorithm.data_key_spec(EncryptionAlgorithm.AES_128_GCM) == "AES_128"
