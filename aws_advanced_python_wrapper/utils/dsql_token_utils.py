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

from typing import TYPE_CHECKING, Dict, Optional

from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.telemetry.telemetry import \
    TelemetryTraceLevel
from aws_advanced_python_wrapper.utils.token_utils import TokenUtils

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from boto3 import Session

import boto3

logger = Logger(__name__)


class DSQLTokenUtils(TokenUtils):
    def generate_authentication_token(
            self,
            plugin_service: PluginService,
            user: Optional[str],
            host_name: Optional[str],
            port: Optional[int],
            region: Optional[str],
            credentials: Optional[Dict[str, str]] = None,
            client_session: Optional[Session] = None) -> str:
        telemetry_factory = plugin_service.get_telemetry_factory()
        context = telemetry_factory.open_telemetry_context("fetch authentication token", TelemetryTraceLevel.NESTED)

        try:
            session = client_session if client_session else boto3.Session()
            if credentials is not None:
                client = session.client(
                    'dsql',
                    region_name=region,
                    aws_access_key_id=credentials.get('AccessKeyId'),
                    aws_secret_access_key=credentials.get('SecretAccessKey'),
                    aws_session_token=credentials.get('SessionToken')
                )
            else:
                client = session.client(
                    'dsql',
                    region_name=region
                )

            if user == "admin":
                token = client.generate_db_connect_admin_auth_token(host_name, region)
            else:
                token = client.generate_db_connect_auth_token(host_name, region)

            logger.debug("IamAuthUtils.GeneratedNewAuthToken", token)
            return token
        except Exception as ex:
            context.set_success(False)
            context.set_exception(ex)
            raise ex
        finally:
            context.close_context()
