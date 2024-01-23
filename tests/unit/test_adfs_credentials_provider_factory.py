#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License").
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable    law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

from unittest.mock import patch

import pytest

from aws_advanced_python_wrapper.federated_plugin import \
    AdfsCredentialsProviderFactory
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

USERNAME = "someFederatedUsername@example.com"
PASSWORD = "somePassword"


@pytest.fixture
def mock_plugin_service(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_request(mocker):
    return mocker.MagicMock()


def test_get_saml_assertion(mock_request, mock_plugin_service):
    properties = Properties()
    WrapperProperties.IDP_ENDPOINT.set(properties, "ec2amaz-ab3cdef.example.com")
    WrapperProperties.IDP_USERNAME.set(properties, USERNAME)
    WrapperProperties.IDP_PASSWORD.set(properties, PASSWORD)
    sign_in_page_html = "./resources/federated_auth/adfs-sign-in-page.html"
    adfs_saml_html = "./resources/federated_auth/adfs-saml.html"
    saml_assertion_txt = "./resources/federated_auth/saml-assertion.txt"

    with open(sign_in_page_html, 'r') as file:
        sign_in_page_body = file.read()
    with open(adfs_saml_html, 'r') as file:
        post_form_action_body = file.read()
    with open(saml_assertion_txt, 'r') as file:
        saml_assertion_expected = file.read()

        with patch('requests.get') as mock_request:
            mock_request.return_value.status_code = 200
            mock_request.return_value.text = sign_in_page_body
            with patch('requests.post') as mock_request:
                mock_request.return_value.status_code = 200
                mock_request.return_value.text = post_form_action_body

                plugin = AdfsCredentialsProviderFactory(mock_plugin_service, properties)
                saml_assertion = plugin.get_saml_assertion(properties)
                assert saml_assertion == saml_assertion_expected

                params = plugin._get_parameters_from_html_body(sign_in_page_body, properties)
                assert params["UserName"] == USERNAME
                assert params["Password"] == PASSWORD
                assert params["Kmsi"] == "true"
                assert params["AuthMethod"] == "FormsAuthentication"
