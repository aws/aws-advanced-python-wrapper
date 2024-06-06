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

from re import search
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from aws_advanced_python_wrapper.errors import AwsWrapperError
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages
from aws_advanced_python_wrapper.utils.properties import (Properties,
                                                          WrapperProperties)

if TYPE_CHECKING:
    from requests import Response


logger = Logger(__name__)


class SamlUtils:
    _HTTPS_URL_PATTERN = r"^(https)://[-a-zA-Z0-9+&@#/%?=~_!:,.']*[-a-zA-Z0-9+&@#/%=~_']"

    @staticmethod
    def check_idp_credentials_with_fallback(props: Properties) -> None:
        if WrapperProperties.IDP_USERNAME.get(props) is None:
            WrapperProperties.IDP_USERNAME.set(props, WrapperProperties.USER.name)
        if WrapperProperties.IDP_PASSWORD.get(props) is None:
            WrapperProperties.IDP_PASSWORD.set(props, WrapperProperties.PASSWORD.name)

    @staticmethod
    def validate_url(url: str) -> None:
        result = urlparse(url)
        if not result.scheme or not search(SamlUtils._HTTPS_URL_PATTERN, url):
            error_message = "SamlUtils.InvalidHttpsUrl"
            logger.debug(error_message, url)
            raise AwsWrapperError(Messages.get_formatted(error_message, url))

    @staticmethod
    def validate_response(response: Response):
        """Validates the HTTP response to ensure the request was successfully executed.

        :param response: HTTP response object
        :raise AwsWrapperError: if response is invalid
        """
        if response.status_code / 100 != 2:
            error_message = "SamlUtils.RequestFailed"
            logger.debug(error_message, response.status_code, response.reason, response.text)
            raise AwsWrapperError(Messages.get_formatted(error_message, response.status_code, response.reason, response.text))
