# Django ORM Support

> [!IMPORTANT]
> Django ORM support is currently only available for **MySQL databases**.

The AWS ADvanced Python Wrapper provides a custom Django database backend that enables Django applications to leverage AWS and Aurora functionalities such as failover handling, IAM authentication, and read/write splitting.

## Prerequisites

- Django 3.2+

## Basic Configuration

To use the AWS ADvanced Python Wrapper with Django, configure your database settings in `settings.py` to use the custom backend in the `ENGINE` parameter, as well as any wrapper-specific properties in the `OPTIONS` parameter:

```python
DATABASES = {
    'default': {
        'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
        'NAME': 'your_database_name',
        'USER': 'your_username',
        'PASSWORD': 'your_password',
        'HOST': 'your-cluster-endpoint.cluster-xyz.us-east-1.rds.amazonaws.com',
        'PORT': 3306,
        'OPTIONS': {
            'plugins': 'failover,iam',
            'connect_timeout': 10,
            'autocommit': True,
        },
    }
}
```

### Supported Engines

| Underlying Driver | Database Dialect | Engine Value |
|-------------------|------------------|-------------|
| `mysql-connector-python` | MySQL | `'aws_advanced_python_wrapper.django.backends.mysql_connector'` |


### OPTIONS Properties

The `OPTIONS` dictionary supports all standard [AWS ADvanced Python Wrapper parameters](./UsingThePythonWrapper.md#aws-advanced-python-driver-parameters) as well as parameters for the underlying driver.

For a complete list of available plugins and their supported parameters, see the [List of Available Plugins](./UsingThePythonWrapper.md#list-of-available-plugins).

## Using Plugins with Django

The AWS ADvanced Python Wrapper supports a variety of plugins that enhance your Django application with features like failover handling, IAM authentication, and more. Most plugins can be enabled simply by adding them to the `plugins` parameter in your database `OPTIONS`.

For a complete list of available plugins, see the [List of Available Plugins](./UsingThePythonWrapper.md#list-of-available-plugins) in the main driver documentation.

Below are two examples of plugins that require additional setup or code changes in your Django application:

### Failover Plugin

The Failover Plugin provides automatic failover handling for Aurora clusters. When a database instance becomes unavailable, the plugin automatically connects to a healthy instance in the cluster.

```python
DATABASES = {
    'default': {
        'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
        'NAME': 'mydb',
        'USER': 'admin',
        'PASSWORD': 'password',
        'HOST': 'my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com',
        'PORT': 3306,
        'OPTIONS': {
            'plugins': 'failover,host_monitoring_v2',
            'connect_timeout': 10,
            'autocommit': True,
        },
    }
}
```

#### Handling Failover Events

During a failover event, the driver will throw a `FailoverSuccessError` exception after successfully connecting to a new instance. Your application should catch this exception and retry the failed query:

```python
from aws_advanced_python_wrapper.errors import FailoverSuccessError

def execute_query_with_failover_handling(query_func):
    try:
        return query_func()
    except FailoverSuccessError:
        # Failover successful, retry the query
        return query_func()
```

For a complete example, see [MySQLDjangoFailover.py](../examples/MySQLDjangoFailover.py).

For more information about the Failover Plugin, see the [Failover Plugin documentation](./using-plugins/UsingTheFailoverPlugin.md).


## Read/Write Splitting with Django

The Read/Write Splitting Plugin enables Django applications to automatically route read queries to reader instances and write queries to writer instances in an Aurora cluster. This plugin requires additional configuration to set up multiple database connections and a database router.

### Configuration

To use read/write splitting with Django:

1. Configure multiple database connections (one for writes, one for reads)
2. Set the `read_only` parameter to `True` for the reader connection to ensure all connection objects route to reader instances
3. Create a database router to direct queries to the appropriate connection

#### Step 1: Configure Database Connections

```python
DATABASES = {
    'default': {  # Writer connection
        'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
        'NAME': 'mydb',
        'USER': 'admin',
        'PASSWORD': 'password',
        'HOST': 'my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com',
        'PORT': 3306,
        'OPTIONS': {
            'plugins': 'read_write_splitting,failover',
            'connect_timeout': 10,
            'autocommit': True,
        },
    },
    'read': {  # Reader connection
        'ENGINE': 'aws_advanced_python_wrapper.django.backends.mysql_connector',
        'NAME': 'mydb',
        'USER': 'admin',
        'PASSWORD': 'password',
        'HOST': 'my-cluster.cluster-xyz.us-east-1.rds.amazonaws.com',
        'PORT': 3306,
        'OPTIONS': {
            'plugins': 'read_write_splitting,failover',
            'connect_timeout': 10,
            'autocommit': True,
            'read_only': True,  # This connection will use reader instances
        },
    },
}
```

#### Step 2: Create a Database Router

Create a database router to direct read operations to the `read` database and write operations to the `default` database:

```python
# myapp/routers.py

class ReadWriteRouter:
    """
    A router to control database operations for read/write splitting.
    """
    
    def db_for_read(self, model, **hints):
        """
        Direct all read operations to the 'read' database.
        """
        return 'read'
    
    def db_for_write(self, model, **hints):
        """
        Direct all write operations to the 'default' database.
        """
        return 'default'
    
    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations between objects in the same database.
        """
        return True
    
    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Allow migrations on all databases.
        """
        return True
```

#### Step 3: Register the Router

Add the router to your Django settings:

```python
DATABASE_ROUTERS = ['myapp.routers.ReadWriteRouter']
```

### Using Read/Write Splitting

Once configured, Django will automatically route queries:

```python
from myapp.models import MyModel

# This will use the 'read' database (reader instance)
objects = MyModel.objects.all()

# This will use the 'default' database (writer instance)
MyModel.objects.create(name='New Object')

# You can also explicitly specify which database to use
MyModel.objects.using('default').all()  # Force use of writer
MyModel.objects.using('read').all()     # Force use of reader
```

### Connection Strategies

By default, the Read/Write Splitting Plugin randomly selects a reader instance. You can configure different connection strategies using the `reader_host_selector_strategy` parameter:

```python
'OPTIONS': {
    'plugins': 'read_write_splitting,failover',
    'reader_host_selector_strategy': 'least_connections',
    'read_only': True,
}
```

For a complete example including connection pooling, see [MySQLDjangoReadWriteSplitting.py](../examples/MySQLDjangoReadWriteSplitting.py).

For a list of available connection strategies, see the [Read/Write Splitting Plugin documentation](./using-plugins/UsingTheReadWriteSplittingPlugin.md#connection-strategies).


## Resource Management

The AWS ADvanced Python Wrapper creates background threads for monitoring and connection management. To ensure proper cleanup when your Django application shuts down, add cleanup code to your application's shutdown process:

```python
# In your Django app's apps.py

from django.apps import AppConfig
from aws_advanced_python_wrapper import release_resources

class MyAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'myapp'
    
    def ready(self):
        # Register cleanup on application shutdown
        import atexit
        atexit.register(release_resources)
```

Or in your WSGI/ASGI application file:

```python
# wsgi.py or asgi.py

import os
from django.core.wsgi import get_wsgi_application
from aws_advanced_python_wrapper import release_resources
import atexit

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myproject.settings')

application = get_wsgi_application()

# Register cleanup
atexit.register(release_resources)
```
