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

from os import walk, path
from re import findall
from typing import List

import pytest
from requests import request


@pytest.fixture
def docs_list():
    doc_list = []

    for root, dirs, files in walk("."):
        for file in files:
            if file.endswith(".md"):
                file_path = str(path.join(root, file))
                file_list = [file_path]
                doc_list = doc_list + file_list
    return doc_list


@pytest.fixture
def urls_list(docs_list: List[str]):
    link_re = r"\((https?://(?!github.com/awslabs/aws-advanced-python-wrapper)[a-zA-Z.\\/-]+)\)"

    new_list: List[str] = []

    for doc in docs_list:
        with open(doc) as f:
            list = findall(link_re, f.read())
            new_list = new_list + list
    return new_list


@pytest.fixture
def relative_links_list(docs_list: List[str]):
    link_re = r"\(([^\S,'\]?!https://]*\.[\S,'\]\)]+)(?=\#)"

    new_list: List[str] = []

    for doc in docs_list:
        with open(doc) as f:
            list = findall(link_re, f.read())
            new_list = new_list + list
    return new_list


def test_verify_urls(urls_list: list):
    for link in urls_list:
        response = request("GET", link)

        assert response.status_code == 200


def test_verify_relative_links(relative_links_list: list):
    for link in relative_links_list:
        assert path.isfile(link)
