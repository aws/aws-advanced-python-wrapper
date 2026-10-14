# Getting Started

## Minimum Requirements

Before using the AWS Advanced Python Wrapper, you must install:

- Python 3.10 - 3.14 (inclusive).
- The AWS Advanced Python Wrapper.
- Your choice of underlying Python driver. 
  - To use the wrapper with Aurora with PostgreSQL compatibility, install [Psycopg](https://github.com/psycopg/psycopg).
  - To use the wrapper with Aurora with MySQL compatibility, install [MySQL Connector/Python](https://github.com/mysql/mysql-connector-python).
> [!NOTE]\
> The wrapper has been verified on Psycopg 3.1.12+ and MySQL Connector/Python 8.1.0+. Compatibility with prior versions have not been tested.

## Obtaining the AWS Advanced Python Wrapper

You can install the AWS Advanced Python Wrapper and the underlying Python drivers via [pip](https://pip.pypa.io/en/stable/).
The order of installation does not matter.

To use the AWS Advanced Python Wrapper with Psycopg for Aurora PostgreSQL, run:

```shell
pip install aws-advanced-python-wrapper
pip install psycopg
```

To use the AWS Advanced Python Wrapper with MySQL Connector/Python for Aurora MySQL, run:
```shell
pip install aws-advanced-python-wrapper
pip install mysql-connector-python
```

To use the wrapper's asyncio API (`aws_advanced_python_wrapper.aio`), install the async driver for your engine. PostgreSQL async uses the same Psycopg package as sync; MySQL async is driven by [aiomysql](https://github.com/aio-libs/aiomysql):

```shell
pip install aws-advanced-python-wrapper
pip install psycopg    # PostgreSQL (sync and async)
pip install aiomysql   # MySQL async
```

Some features pull in additional packages when used with the asyncio API:

- The async Federated Authentication and Okta Authentication plugins use [aiohttp](https://docs.aiohttp.org/) for their HTTP flows: `pip install aiohttp`.
- Async SQLAlchemy (`create_async_engine`) requires SQLAlchemy's asyncio support, which includes `greenlet`: `pip install "sqlalchemy[asyncio]"`.

### Asyncio on Windows

Python's default event loop on Windows is `ProactorEventLoop`, and Psycopg's async
connection refuses to run on it:

```
psycopg.InterfaceError: Psycopg cannot use the 'ProactorEventLoop' to run in async mode
```

Every async PostgreSQL connection fails at connect until a selector event loop is
selected. Do this once, before any async code runs:

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

This is a requirement of the underlying driver rather than of the wrapper, and it does
not apply on Linux or macOS, where the default loop is already a selector loop. See
[Psycopg's asynchronous operations documentation](https://www.psycopg.org/psycopg3/docs/advanced/async.html#async).

## Using the AWS Advanced Python Wrapper

To start using the wrapper with Psycopg, you need to pass Psycopg's connect function to the `AwsWrapperConnection#connect` method as shown in the following example:

```python
from aws_advanced_python_wrapper import AwsWrapperConnection
from psycopg import Connection

awsconn = AwsWrapperConnection.connect(
    Connection.connect,
    "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd",
    plugins="failover",
    autocommit=True
)
```

Similarly, to start using the wrapper with MySQL Connector/Python, you need to pass the connect function to the `AwsWrapperConnection#connect` method as shown in the following example:
```python
awsconn = AwsWrapperConnection.connect(
        mysql.connector.Connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        database="mysql",
        user="admin",
        password="pwd",
        plugins="failover",
        autocommit=True
)
```

The `AwsWrapperConnection#connect` method accepts the connection configuration through both the connection string and the keyword arguments.
You can either pass the connection configuration entirely through the connection string, entirely though the keyword arguments, or through both the connection string and the keywords arguments as shown below.

**Configuring the connection using the connection string**
```python
awsconn = AwsWrapperConnection.connect(
        Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd plugins=failover wrapper_dialect=aurora-pg"
)
```

**Configuring the connection using the keyword arguments**
```python
awsconn = AwsWrapperConnection.connect(
        Connection.connect,
        host="database.cluster-xyz.us-east-1.rds.amazonaws.com",
        dbname="postgres",
        user="john",
        password="pwd",
        plugins="failover",
        wrapper_dialect="aurora-pg"
)
```

> **NOTE**: If the same configuration is specified in both the connection string and the keyword arguments, the keyword argument takes precedence.

The AWS Advanced Python Wrapper implements the [PEP 249 Database API](https://peps.python.org/pep-0249/).
After establishing a connection, you can use it in the same pattern as you would with the community Python drivers.
However, the wrapper introduces some custom errors for the Failover Plugin and the Read/Write Splitting Plugin that need to be explicitly handled.

For instance, after a successful failover, some session states may not be transferred to the new connection, so the wrapper throws a `FailoverSuccessError` to notify the application that the connection may need to be reconfigured, or to create a new cursor object.
See this simple PostgreSQL example:

```python
import psycopg
from aws_advanced_python_wrapper import AwsWrapperConnection
from aws_advanced_python_wrapper.errors import FailoverSuccessError

with AwsWrapperConnection.connect(
        psycopg.Connection.connect,
        "host=database.cluster-xyz.us-east-1.rds.amazonaws.com dbname=db user=john password=pwd"
) as awsconn:
    try:
        with awsconn.cursor() as cursor:
            cursor.execute(sql)

    except FailoverSuccessError:
        # Query execution failed and AWS Advanced Python Wrapper successfully failed over to an available instance.
        # The old cursor is no longer reusable and the application needs to reconfigure sessions states.
        reconfigure_session_states(awsconn)

        # Retry query
        with awsconn.cursor() as cursor:
            cursor.execute(sql)
```
A full PostgreSQL example is available at [PGFailover.py](./examples/PGFailover.py). A full MySQL example, [MySQLFailover.py](./examples/MySQLFailover.py), is available as well.

You can learn more about the AWS Advanced Python Wrapper specific errors in the [Using the Failover Plugin](using-the-python-wrapper/using-plugins/UsingTheFailoverPlugin.md#Failover-Errors) page.

For more detailed information about how to use and configure the AWS Advanced Python Wrapper, please visit [this page](using-the-python-wrapper/UsingThePythonWrapper.md).

## Configuring the Wrapper with AI Tools

If you use an AI coding assistant (Kiro, Cursor, Claude Code, GitHub Copilot, ChatGPT, Amazon Bedrock, etc.), you can use the wrapper's configuration assistant skill to help build, review, or troubleshoot your wrapper configuration.

The skill is a single self-contained markdown file: [`PYTHON-WRAPPER-CONFIGURATION-ASSISTANT.md`](./PYTHON-WRAPPER-CONFIGURATION-ASSISTANT.md). It covers every plugin, parameter, default value, dialect, failover mode, and compatibility constraint of release 3.1.0, together with recipes for psycopg, MySQL Connector/Python, SQLAlchemy (sync and async), and Django, so the AI can answer configuration questions without access to the wrapper source.

The file follows the [Agent Skills](https://agentskills.io) format. It is large: about 100,000 tokens on Claude Opus 5.5 (other models count tokens differently), and the AI receives it again with every message. Tools that cache the system prompt handle it far more cheaply than tools that resend it in full each time.

### Quick start

1. Download or copy [`PYTHON-WRAPPER-CONFIGURATION-ASSISTANT.md`](./PYTHON-WRAPPER-CONFIGURATION-ASSISTANT.md).
2. Add it to your AI tool:
   - **Tools that support Agent Skills** (Kiro, Cursor, Claude Code, GitHub Copilot in VS Code, and others): create a folder named `python-wrapper-configuration-assistant` in one of the tool's skills directories, and save the file inside it as `SKILL.md`. The folder name must match the `name` field at the top of the file; otherwise the tool does not load the skill.

     | Tool | Project skills directory | User skills directory |
     |---|---|---|
     | Kiro | `.kiro/skills/` | `~/.kiro/skills/` |
     | Cursor | `.cursor/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.cursor/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |
     | Claude Code | `.claude/skills/` | `~/.claude/skills/` |
     | GitHub Copilot in VS Code | `.github/skills/`, `.agents/skills/`, or `.claude/skills/` | `~/.copilot/skills/`, `~/.agents/skills/`, or `~/.claude/skills/` |

     For example, to add the skill to a project for Claude Code, Cursor, and VS Code at once:

     ```bash
     mkdir -p .claude/skills/python-wrapper-configuration-assistant
     curl -fsSL -o .claude/skills/python-wrapper-configuration-assistant/SKILL.md \
       https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/docs/PYTHON-WRAPPER-CONFIGURATION-ASSISTANT.md
     ```

   - **Claude Projects**: upload it as a project knowledge document.
   - **ChatGPT custom GPT**: upload it as a knowledge file.
   - **Amazon Bedrock**: send the file as the system prompt of your `InvokeModel` or `Converse` request. Avoid attaching it to a Bedrock knowledge base: a knowledge base passes the model only the passages that match each question, so the skill's interview steps and rules (section 1 of the file) may be missing.
   - **Plain chat (ChatGPT, Claude, Gemini)**: paste it at the start of a new conversation, then ask your question.
3. Ask the AI for help. It will interview you when it needs more details, or give you a configuration straight away if you ask it to skip the interview.

### Example prompts

The simplest way to start:

```
Help me configure the AWS Advanced Python Wrapper for my application.
```

Skip the interview when you already know what you want:

```
Give me a default configuration for Aurora PostgreSQL with SQLAlchemy, psycopg, and failover support.
```

Review an existing configuration:

```
Review this wrapper configuration and tell me what to change:

<paste your AwsWrapperConnection.connect call, SQLAlchemy engine setup, or Django DATABASES setting here>
```

Diagnose an error:

```
My Django app on Aurora MySQL fails to connect with "[HostMonitoringV2Plugin] Aborting
connections from a separate thread is not supported for the detected driver dialect:
'MySQLDriverDialect'". What's wrong?
```

The skill targets release 3.1.0 and avoids guessing: if it doesn't know something, it says so and points you at the official docs.
