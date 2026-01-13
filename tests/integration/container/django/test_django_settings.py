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

"""
Django settings for Django integration tests.
This file is used by the Django test framework.
"""

# This is a minimal Django settings file for testing purposes
# The actual database configuration is done dynamically in the test fixtures

DEBUG = True

SECRET_KEY = 'test-secret-key-for-django-integration-tests'

INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
]

# Database configuration will be set dynamically in test fixtures
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}

USE_TZ = True

# Minimal configuration for testing
ROOT_URLCONF = []