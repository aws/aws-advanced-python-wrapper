rootProject.name = "aws-advanced-python-wrapper"

include("integration-testing")

project(":integration-testing").projectDir = file("tests/integration/host")
