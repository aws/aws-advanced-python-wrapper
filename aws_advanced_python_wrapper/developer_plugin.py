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

from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, Set

if TYPE_CHECKING:
    from aws_advanced_python_wrapper.driver_dialect import DriverDialect
    from aws_advanced_python_wrapper.plugin_service import PluginService
    from aws_advanced_python_wrapper.pep249 import Connection
    from aws_advanced_python_wrapper.utils.properties import Properties
    from aws_advanced_python_wrapper.hostinfo import HostInfo

from aws_advanced_python_wrapper.plugin import Plugin, PluginFactory
from aws_advanced_python_wrapper.utils.log import Logger
from aws_advanced_python_wrapper.utils.messages import Messages

logger = Logger(__name__)


class ExceptionSimulatorConnectCallback(Protocol):
    def get_exception_to_raise(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Any:
        ...


class ExceptionSimulatorMethodCallback(Protocol):
    def get_exception_to_raise(
            self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        ...


class ExceptionSimulatorManager:
    next_connect_exception: Optional[Exception] = None
    connect_callback: Optional[ExceptionSimulatorConnectCallback] = None
    next_method_name: Optional[str] = None
    next_method_exception: Optional[Exception] = None
    method_callback: Optional[ExceptionSimulatorMethodCallback] = None

    @staticmethod
    def raise_exception_on_next_connect(error: Exception) -> None:
        ExceptionSimulatorManager.next_connect_exception = error

    @staticmethod
    def set_connect_callback(connect_callback: Optional[ExceptionSimulatorConnectCallback]) -> None:
        ExceptionSimulatorManager.connect_callback = connect_callback

    @staticmethod
    def raise_exception_on_next_method(error: Exception, method_name: str = "*") -> None:
        if method_name == "":
            raise RuntimeError(Messages.get("DeveloperPlugin.MethodNameEmpty"))
        ExceptionSimulatorManager.next_method_exception = error
        ExceptionSimulatorManager.next_method_name = method_name

    @staticmethod
    def set_method_callback(method_callback: Optional[ExceptionSimulatorMethodCallback]) -> None:
        ExceptionSimulatorManager.method_callback = method_callback


class DeveloperPlugin(Plugin):
    _ALL_METHODS: str = "*"
    _SUBSCRIBED_METHODS: Set[str] = {_ALL_METHODS}

    @property
    def subscribed_methods(self) -> Set[str]:
        return DeveloperPlugin._SUBSCRIBED_METHODS

    def execute(self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> Any:
        self.raise_method_exception_if_set(target, method_name, execute_func, *args, **kwargs)
        return execute_func()

    def raise_method_exception_if_set(
            self, target: type, method_name: str, execute_func: Callable, *args: Any, **kwargs: Any) -> None:
        if ExceptionSimulatorManager.next_method_exception is not None:
            if DeveloperPlugin._ALL_METHODS == ExceptionSimulatorManager.next_method_name or \
                    method_name == ExceptionSimulatorManager.next_method_name:
                self.raise_exception_on_method(ExceptionSimulatorManager.next_method_exception, method_name)
        elif ExceptionSimulatorManager.method_callback is not None:
            exception = ExceptionSimulatorManager.method_callback.get_exception_to_raise(
                target, method_name, execute_func, *args, **kwargs)
            self.raise_exception_on_method(exception, method_name)

    def raise_exception_on_method(self, error: Optional[Exception], method_name: str) -> None:
        if error is None:
            return

        ExceptionSimulatorManager.next_method_exception = None
        ExceptionSimulatorManager.next_method_name = None
        logger.debug("DeveloperPlugin.RaisedExceptionWhileExecuting", error.__class__.__name__, method_name)
        raise error

    def connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> Connection:
        self.raise_connect_exception_if_set(
            target_driver_func, driver_dialect, host_info, props, is_initial_connection, connect_func)
        return super().connect(
            target_driver_func, driver_dialect, host_info, props, is_initial_connection, connect_func)

    def force_connect(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            force_connect_func: Callable) -> Connection:
        self.raise_connect_exception_if_set(
            target_driver_func, driver_dialect, host_info, props, is_initial_connection, force_connect_func)
        return super().connect(
            target_driver_func, driver_dialect, host_info, props, is_initial_connection, force_connect_func)

    def raise_connect_exception_if_set(
            self,
            target_driver_func: Callable,
            driver_dialect: DriverDialect,
            host_info: HostInfo,
            props: Properties,
            is_initial_connection: bool,
            connect_func: Callable) -> None:
        if ExceptionSimulatorManager.next_connect_exception is not None:
            self.raise_exception_on_connect(ExceptionSimulatorManager.next_connect_exception)
        elif ExceptionSimulatorManager.connect_callback is not None:
            exception = ExceptionSimulatorManager.connect_callback.get_exception_to_raise(
                target_driver_func, driver_dialect, host_info, props, is_initial_connection, connect_func)
            self.raise_exception_on_connect(exception)

    def raise_exception_on_connect(self, error: Optional[Exception]) -> None:
        if error is None:
            return

        ExceptionSimulatorManager.next_connect_exception = None
        logger.debug("DeveloperPlugin.RaisedExceptionOnConnect", error.__class__.__name__)
        raise error


class DeveloperPluginFactory(PluginFactory):

    def get_instance(self, plugin_service: PluginService, props: Properties) -> Plugin:
        return DeveloperPlugin()
