"""Read an explicitly named local credential file after verifying its permissions."""

from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from functools import partial
import os
from pathlib import Path
import re
import stat


_MAX_FILE_SIZE = 8192
_KEY = re.compile(rb"[A-Za-z0-9._~+/=\-]{16,1024}")
_FORMAT_ERROR = "Invalid credential format."
_FILE_ERROR = "Credential file could not be read securely."


def normalize_key(raw: bytes) -> bytes:
    if not isinstance(raw, bytes) or len(raw) > _MAX_FILE_SIZE:
        raise ValueError(_FORMAT_ERROR)
    raw = raw.removeprefix(b"\xef\xbb\xbf")
    if raw.endswith(b"\r\n"):
        raw = raw[:-2]
    elif raw.endswith(b"\n"):
        raw = raw[:-1]
    if not _KEY.fullmatch(raw):
        raise ValueError(_FORMAT_ERROR)
    return raw


def read_protected_file(path: str | os.PathLike[str]) -> bytes:
    try:
        with _pin_protected_file(path) as read:
            return read()
    except (OSError, TypeError, ValueError, AttributeError, OverflowError):
        raise ValueError(_FILE_ERROR) from None


@contextmanager
def pinned_protected_file(path: str | os.PathLike[str]) -> Iterator[None]:
    """Verify metadata and hold the path open without reading the file contents.

    On POSIX, holding descriptors does not prevent pathname changes or concurrent
    writes by the same UID. This check does not isolate a later pathname consumer.
    """
    with _pin_protected_file(path):
        yield


@contextmanager
def _pin_protected_file(path):
    with ExitStack() as handles:
        try:
            path = Path(path)
            if not path.is_absolute() or len(path.parts) < 2 or ".." in path.parts:
                raise ValueError
            pin = _WindowsReader().pin if os.name == "nt" else _pin_posix
            read = handles.enter_context(pin(path))
        except (OSError, TypeError, ValueError, AttributeError, OverflowError):
            raise ValueError(_FILE_ERROR) from None
        yield read


@contextmanager
def _pin_posix(path: Path):
    if path.anchor != "/":
        raise ValueError
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    with ExitStack() as handles:
        parent = os.open(path.anchor, directory_flags)
        handles.callback(os.close, parent)
        for part in path.parts[1:-1]:
            parent = os.open(part, directory_flags, dir_fd=parent)
            handles.callback(os.close, parent)
        descriptor = os.open(
            path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
        )
        handles.callback(os.close, descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) not in (0o400, 0o600)
            or before.st_nlink != 1
            or not 0 <= before.st_size <= _MAX_FILE_SIZE
        ):
            raise ValueError
        yield partial(_read_posix, descriptor, before)


def _read_posix(descriptor, before):
    raw = bytearray()
    while len(raw) <= _MAX_FILE_SIZE:
        chunk = os.read(descriptor, _MAX_FILE_SIZE + 1 - len(raw))
        if not chunk:
            break
        raw.extend(chunk)
    after = os.fstat(descriptor)
    if (
        len(raw) != before.st_size
        or after.st_mode != before.st_mode
        or after.st_uid != before.st_uid
        or after.st_nlink != before.st_nlink
        or after.st_size != before.st_size
        or after.st_ctime_ns != before.st_ctime_ns
        or after.st_mtime_ns != before.st_mtime_ns
    ):
        raise ValueError
    return bytes(raw)


if os.name == "nt":
    import ctypes as ct
    from ctypes import wintypes as w

    _GENERIC_READ = 0x80000000
    _READ_CONTROL = 0x00020000
    _FILE_READ_ATTRIBUTES = 0x0080
    _FILE_SHARE_READ = 0x01
    _OPEN_EXISTING = 3
    _FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
    _FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
    _FILE_ATTRIBUTE_DIRECTORY = 0x10
    _FILE_ATTRIBUTE_REPARSE_POINT = 0x400
    _TOKEN_QUERY = 0x0008
    _TOKEN_USER = 1
    _SE_FILE_OBJECT = 1
    _OWNER_SECURITY_INFORMATION = 0x01
    _DACL_SECURITY_INFORMATION = 0x04
    _SE_DACL_PROTECTED = 0x1000
    _ACCESS_ALLOWED_ACE_TYPE = 0
    _ACCESS_DENIED_ACE_TYPE = 1
    _WIN_LOCAL_SYSTEM_SID = 22
    _INVALID_HANDLE = ct.c_void_p(-1).value

    class _FileInformation(ct.Structure):
        _fields_ = [
            ("attributes", w.DWORD),
            ("creation_time", w.FILETIME),
            ("last_access_time", w.FILETIME),
            ("last_write_time", w.FILETIME),
            ("volume_serial_number", w.DWORD),
            ("size_high", w.DWORD),
            ("size_low", w.DWORD),
            ("links", w.DWORD),
            ("file_index_high", w.DWORD),
            ("file_index_low", w.DWORD),
        ]

    class _Acl(ct.Structure):
        _fields_ = [
            ("revision", w.BYTE),
            ("reserved", w.BYTE),
            ("size", w.WORD),
            ("ace_count", w.WORD),
            ("reserved2", w.WORD),
        ]

    class _AceHeader(ct.Structure):
        _fields_ = [("type", w.BYTE), ("flags", w.BYTE), ("size", w.WORD)]

    class _WindowsReader:
        def __init__(self):
            kernel = ct.WinDLL("kernel32", use_last_error=True)
            security = ct.WinDLL("advapi32", use_last_error=True)
            pointer = ct.c_void_p
            pointer_ref = ct.POINTER(pointer)
            dword_ref = ct.POINTER(w.DWORD)
            self.open_file = self._api(
                kernel, "CreateFileW", w.HANDLE,
                w.LPCWSTR, w.DWORD, w.DWORD, pointer, w.DWORD, w.DWORD, w.HANDLE,
            )
            self.drive_type = self._api(kernel, "GetDriveTypeW", w.UINT, w.LPCWSTR)
            self.close = self._api(kernel, "CloseHandle", w.BOOL, w.HANDLE)
            self.info = self._api(
                kernel, "GetFileInformationByHandle", w.BOOL,
                w.HANDLE, ct.POINTER(_FileInformation),
            )
            self.read_file = self._api(
                kernel, "ReadFile", w.BOOL, w.HANDLE, pointer, w.DWORD, dword_ref, pointer
            )
            self.free = self._api(kernel, "LocalFree", w.HLOCAL, w.HLOCAL)
            self.current_process = self._api(kernel, "GetCurrentProcess", w.HANDLE)
            self.open_token = self._api(
                security, "OpenProcessToken", w.BOOL, w.HANDLE, w.DWORD, pointer_ref
            )
            self.token_info = self._api(
                security, "GetTokenInformation", w.BOOL,
                w.HANDLE, ct.c_int, pointer, w.DWORD, dword_ref,
            )
            self.security_info = self._api(
                security, "GetSecurityInfo", w.DWORD,
                w.HANDLE, ct.c_int, w.DWORD, pointer_ref, pointer_ref,
                pointer_ref, pointer_ref, pointer_ref,
            )
            self.descriptor_control = self._api(
                security, "GetSecurityDescriptorControl", w.BOOL,
                pointer, ct.POINTER(w.WORD), dword_ref,
            )
            self.equal_sid = self._api(security, "EqualSid", w.BOOL, pointer, pointer)
            self.well_known_sid = self._api(
                security, "IsWellKnownSid", w.BOOL, pointer, ct.c_int
            )
            self.valid_acl = self._api(security, "IsValidAcl", w.BOOL, pointer)
            self.valid_sid = self._api(security, "IsValidSid", w.BOOL, pointer)
            self.sid_to_string = self._api(
                security, "ConvertSidToStringSidW", w.BOOL, pointer, pointer_ref
            )
            self.get_ace = self._api(
                security, "GetAce", w.BOOL, pointer, w.DWORD, pointer_ref
            )

        @staticmethod
        def _api(library, name, result, *arguments):
            function = getattr(library, name)
            function.argtypes = arguments
            function.restype = result
            return function

        def _open(self, stack, path, directory=False):
            access = _FILE_READ_ATTRIBUTES if directory else _GENERIC_READ | _READ_CONTROL
            flags = _FILE_FLAG_OPEN_REPARSE_POINT
            if directory:
                flags |= _FILE_FLAG_BACKUP_SEMANTICS
            handle = self.open_file(
                "\\\\?\\" + str(path), access, _FILE_SHARE_READ,
                None, _OPEN_EXISTING, flags, None,
            )
            if handle in (None, _INVALID_HANDLE):
                raise ValueError
            stack.callback(self.close, handle)
            return handle

        def _info(self, handle, directory=False):
            info = _FileInformation()
            if not self.info(handle, ct.byref(info)):
                raise ValueError
            if (
                info.attributes & _FILE_ATTRIBUTE_REPARSE_POINT
                or bool(info.attributes & _FILE_ATTRIBUTE_DIRECTORY) != directory
            ):
                raise ValueError
            return info

        def _user_token(self, stack):
            token = w.HANDLE()
            if not self.open_token(self.current_process(), _TOKEN_QUERY, ct.byref(token)):
                raise ValueError
            stack.callback(self.close, token)
            size = w.DWORD()
            self.token_info(token, _TOKEN_USER, None, 0, ct.byref(size))
            if not 0 < size.value <= _MAX_FILE_SIZE:
                raise ValueError
            data = ct.create_string_buffer(size.value)
            if not self.token_info(token, _TOKEN_USER, data, size, ct.byref(size)):
                raise ValueError
            return data

        def _check_acl(self, handle, user_sid):
            owner, dacl, descriptor = ct.c_void_p(), ct.c_void_p(), ct.c_void_p()
            status = self.security_info(
                handle, _SE_FILE_OBJECT,
                _OWNER_SECURITY_INFORMATION | _DACL_SECURITY_INFORMATION,
                ct.byref(owner), None, ct.byref(dacl), None, ct.byref(descriptor),
            )
            try:
                if status or not descriptor or not owner or not dacl:
                    raise ValueError
                if not self.equal_sid(owner, user_sid) or not self.valid_acl(dacl):
                    raise ValueError
                control, revision = w.WORD(), w.DWORD()
                if not self.descriptor_control(
                    descriptor, ct.byref(control), ct.byref(revision)
                ) or not control.value & _SE_DACL_PROTECTED:
                    raise ValueError
                for index in range(_Acl.from_address(dacl.value).ace_count):
                    self._check_ace(dacl, index, user_sid)
            finally:
                if descriptor:
                    self.free(descriptor)

        def _check_ace(self, dacl, index, user_sid):
            ace = ct.c_void_p()
            if not self.get_ace(dacl, index, ct.byref(ace)):
                raise ValueError
            header = _AceHeader.from_address(ace.value)
            if header.type not in (_ACCESS_ALLOWED_ACE_TYPE, _ACCESS_DENIED_ACE_TYPE):
                raise ValueError
            # Standard access ACE body is a DWORD mask followed by the SID.
            sid_start = ct.sizeof(_AceHeader) + ct.sizeof(w.DWORD)
            if header.size < sid_start + 8:
                raise ValueError
            sid = ace.value + sid_start
            sid_size = 8 + 4 * w.BYTE.from_address(sid + 1).value
            if sid_start + sid_size > header.size or not self.valid_sid(sid):
                raise ValueError
            if header.type == _ACCESS_ALLOWED_ACE_TYPE and not (
                self.equal_sid(sid, user_sid)
                or self.well_known_sid(sid, _WIN_LOCAL_SYSTEM_SID)
            ):
                raise ValueError

        @contextmanager
        def pin(self, path):
            if (
                not re.fullmatch(r"[A-Za-z]:", path.drive)
                or self.drive_type(path.anchor) not in (2, 3, 6)
                or any(
                    part.endswith((".", " ")) or re.search(r'[\x00-\x1f<>:"|?*]', part)
                    for part in path.parts[1:]
                )
            ):
                raise ValueError
            with ExitStack() as handles:
                # Keep every ancestor open without delete sharing while the path is in use.
                ancestors = []
                for parent in reversed(path.parents):
                    ancestor = self._open(handles, parent, directory=True)
                    self._info(ancestor, directory=True)
                    ancestors.append(ancestor)
                handle = self._open(handles, path)
                for ancestor in ancestors:
                    self._info(ancestor, directory=True)
                before = self._info(handle)
                if before.links != 1 or before.size_high or before.size_low > _MAX_FILE_SIZE:
                    raise ValueError
                token = self._user_token(handles)
                user_sid = ct.cast(token, ct.POINTER(ct.c_void_p))[0]
                self._check_acl(handle, user_sid)
                yield partial(self._read, handle, before, user_sid)

        def _read(self, handle, before, user_sid):
            data = ct.create_string_buffer(_MAX_FILE_SIZE + 1)
            length = w.DWORD()
            if not self.read_file(handle, data, len(data), ct.byref(length), None):
                raise ValueError
            after = self._info(handle)
            if (
                length.value != before.size_low
                or after.links != 1
                or after.size_high
                or after.size_low != before.size_low
                or after.last_write_time.dwHighDateTime != before.last_write_time.dwHighDateTime
                or after.last_write_time.dwLowDateTime != before.last_write_time.dwLowDateTime
            ):
                raise ValueError
            self._check_acl(handle, user_sid)
            return data.raw[:length.value]

    def current_user_sid() -> str:
        """Return the SID of the user in the current process token."""
        try:
            reader = _WindowsReader()
            with ExitStack() as handles:
                token = reader._user_token(handles)
                sid = ct.cast(token, ct.POINTER(ct.c_void_p))[0]
                result = ct.c_void_p()
                if not reader.sid_to_string(sid, ct.byref(result)):
                    raise ValueError
                handles.callback(reader.free, result)
                return ct.wstring_at(result)
        except (OSError, TypeError, ValueError, AttributeError, OverflowError):
            raise ValueError("Current user SID could not be determined.") from None
