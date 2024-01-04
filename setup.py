from distutils.core import setup

setup(
    name='aws-advanced-python-wrapper',
    version='1.0.0',
    packages=['tests', 'tests.unit', 'tests.unit.utils', 'tests.integration', 'tests.integration.container',
              'tests.integration.container.utils', 'tests.integration.container.scripts', 'benchmarks',
              'aws_advanced_python_wrapper', 'aws_advanced_python_wrapper.utils'],
    url='https://github.com/awslabs/aws-advanced-python-wrapper',
    license='Apache License 2.0',
    author='Amazon Web Services',
    author_email='',
    description='Amazon Web Services (AWS) Advanced Python Wrapper'
)
