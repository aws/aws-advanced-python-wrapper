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

from typing import TYPE_CHECKING, Dict, Optional, Protocol

import boto3

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.utils.properties import Properties

from abc import abstractmethod

from aws_advanced_python_wrapper.utils.properties import WrapperProperties


class CredentialsProviderFactory(Protocol):
    @abstractmethod
    def get_aws_credentials(self, region: str, props: Properties) -> Optional[Dict[str, str]]:
        ...


class SamlCredentialsProviderFactory(CredentialsProviderFactory):

    def get_aws_credentials(self, region: str, props: Properties) -> Optional[Dict[str, str]]:
        saml_assertion: str = self.get_saml_assertion(props)
        session = boto3.Session()

        sts_client = session.client(
            'sts',
            region_name=region
        )

        response: Dict[str, Dict[str, str]] = sts_client.assume_role_with_saml(
            RoleArn=WrapperProperties.IAM_ROLE_ARN.get(props),
            PrincipalArn=WrapperProperties.IAM_IDP_ARN.get(props),
            SAMLAssertion=saml_assertion,
        )

        return response.get('Credentials')

    def get_saml_assertion(self, props: Properties):
        ...
