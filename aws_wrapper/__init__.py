"""
pawswrapper -- AWS Wrapper driver for Python
"""

from .wrapper import AwsWrapperConnection

# PEP249 compliance
connect = AwsWrapperConnection.connect
apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"
