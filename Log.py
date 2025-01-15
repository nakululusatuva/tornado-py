import datetime
import inspect
import os
import sys
import threading as TR
from typing import Any, TextIO

from MyType import MBytes


class STDOutStreamWrapper(object):

    def __init__(self, stream: TextIO):
        self.mutex : TR.Lock = TR.Lock()
        self.stream: TextIO  = stream

    def write(self, buf: Any):
        with self.mutex:
            self.stream.write(buf)


STDOUT: STDOutStreamWrapper = STDOutStreamWrapper(sys.stdout)
STDERR: STDOutStreamWrapper = STDOutStreamWrapper(sys.stderr)


def CallerLocation(func) -> Any:
    def wrapper(*args, **kwargs) -> Any:
        stack       = inspect.currentframe().f_back
        filename    = os.path.basename(stack.f_code.co_filename)
        line_number = stack.f_lineno
        kwargs['filename']    = filename
        kwargs['line_number'] = line_number
        return func(*args, **kwargs)
    return wrapper


class Logger(object):
    def __init__(self, directory: str, name: str, rotate_size: MBytes = 0):
        os.makedirs(directory, exist_ok=True)
        self.directory  : str     = directory
        self.name       : str     = name
        self.first_file : bool    = True
        self.file       : TextIO  = self.open_file()
        self.stdout     : bool    = False
        self.mutex      : TR.Lock = TR.Lock()
        self.rotate_size: MBytes  = rotate_size

    def __del__(self):
        self.file.close()

    def console(self, enable: bool) -> None:
        with self.mutex:
            self.stdout = enable

    def log(self, level: str, tag: str, message: str|list[str], filename: str|None, line_number: int|None) -> None:
        with self.mutex:
            batch: list[str] = []
            if isinstance(message, str):
                batch.append(message)
            elif isinstance(message, list):
                batch = message
            for msg in batch:
                text: str = f'[{level} {self.now_str_log()} tid={TR.get_native_id()} {tag}] {msg}'
                if filename is not None and line_number is not None:
                    text += f' ({filename}:{line_number})'
                self.file.write(f'{text}\n')
                if self.stdout:
                    STDOUT.write(f'{text}\n')
            if 0 != self.rotate_size and self.file_size() >= self.rotate_size:
                self.file.close()
                self.file = self.open_file()

    def now_str_filename(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

    def now_str_log(self) -> str:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S:%f')[:-3]

    def file_size(self) -> MBytes:
        return MBytes(int(os.fstat(self.file.fileno()).st_size / 1024.0 / 1024.0))

    def open_file(self) -> TextIO:
        filename: str = f'{self.name}_{self.now_str_filename()}'
        if self.name == '':
            filename = self.now_str_filename()
        if self.first_file:
            self.first_file = False
            filename = f'{filename}_start'
        return open(file=f'{self.directory}/{filename}.log', mode='a', buffering=1)


class INSTANCE:

    init: bool   = False
    obj : Logger = None

    @classmethod
    def Init(cls, directory: str, name: str, rotate_size: MBytes = 0) -> bool:
        if INSTANCE.init:
            return False
        INSTANCE.obj = Logger(directory, name, rotate_size)
        INSTANCE.init = True
        return True

    @classmethod
    def UnInit(cls) -> bool:
        if INSTANCE.init:
            INSTANCE.obj = None
            INSTANCE.init = False
            return True
        return False

    @classmethod
    def Console(cls, enable: bool) -> None:
        if INSTANCE.obj is None:
            raise Exception('Log.Console() obj is None')
        INSTANCE.obj.console(enable)

    @classmethod
    def Info(cls, tag: str, message: str|list[str], filename: str|None, line_number: int|None) -> None:
        if INSTANCE.obj is None:
            raise Exception('Log.Info() obj is None')
        INSTANCE.obj.log('I', tag, message, filename, line_number)

    @classmethod
    def Error(cls, tag: str, message: str|list[str], filename: str|None, line_number: int|None) -> None:
        if INSTANCE.obj is None:
            raise Exception('Log.Error() obj is None')
        INSTANCE.obj.log('E', tag, message, filename, line_number)

    @classmethod
    def Warn(cls, tag: str, message: str|list[str], filename: str|None, line_number: int|None) -> None:
        if INSTANCE.obj is None:
            raise Exception('Log.Warn() obj is None')
        INSTANCE.obj.log('W', tag, message, filename, line_number)

    @classmethod
    def Debug(cls, tag: str, message: str|list[str], filename: str|None, line_number: int|None) -> None:
        if INSTANCE.obj is None:
            raise Exception('Log.Debug() obj is None')
        INSTANCE.obj.log('D', tag, message, filename, line_number)


def Init(directory: str, name: str, rotate_size: MBytes = 0) -> bool:
    return INSTANCE.Init(directory, name, rotate_size)


def UnInit() -> bool:
    return INSTANCE.UnInit()


def Console(enable: bool) -> None:
    INSTANCE.Console(enable)


"""
Fake file-like stream object that redirects writes to a logger instance.
"""
class STDOutStreamRelay(object):

    def __init__(self, level: str):
        self.mutex   : TR.Lock = TR.Lock()
        self.level   : str     = level
        self.buffer  : str     = ''

    def flush(self):
        pass

    def write(self, buf: Any):
        text   : str = str(buf)
        reserve: bool = not text.endswith('\n')
        with self.mutex:
            # Append buffer
            self.buffer += str(buf)
            # Remove '\n' at the end of buffer, because it will cause an extra '' in variable lines
            if self.buffer.endswith('\n'):
                self.buffer = self.buffer[:-1]
            # Split buffer into lines by '\n'
            lines: list[str] = self.buffer.split('\n')
            lines = [line.strip('\n') for line in lines]
            # Reserve last line if it is not a completed line
            if reserve:
                self.buffer = lines.pop(len(lines) - 1)
            else:
                self.buffer = ''
        if 'I' == self.level:
            INSTANCE.Info('stdout', lines, None, None)
        elif 'E' == self.level:
            INSTANCE.Error('stderr', lines, None, None)


sys.stdout = STDOutStreamRelay('I')
sys.stderr = STDOutStreamRelay('E')


@CallerLocation
def Info(tag: str, message: str|list[str], filename: str = None, line_number: int = None) -> None:
    if filename is None or line_number is None:
        raise Exception('Log.Info() filename or line_number is None')
    INSTANCE.Info(tag, message, filename, line_number)


@CallerLocation
def Error(tag: str, message: str|list[str], filename: str = None, line_number: int = None) -> None:
    if filename is None or line_number is None:
        raise Exception('Log.Error() filename or line_number is None')
    INSTANCE.Error(tag, message, filename, line_number)


@CallerLocation
def Warn(tag: str, message: str|list[str], filename: str = None, line_number: int = None) -> None:
    if filename is None or line_number is None:
        raise Exception('Log.Warn() filename or line_number is None')
    INSTANCE.Warn(tag, message, filename, line_number)


@CallerLocation
def Debug(tag: str, message: str|list[str], filename: str = None, line_number: int = None) -> None:
    if filename is None or line_number is None:
        raise Exception('Log.Debug() filename or line_number is None')
    INSTANCE.Debug(tag, message, filename, line_number)


def Print(message: str|list[str]) -> None:
    if isinstance(message, str):
        STDOUT.write(f'{message}\n')
    elif isinstance(message, list):
        for msg in message:
            STDOUT.write(f'{msg}\n')
