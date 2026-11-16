"""One-shot QEMU lab credential delivery over an unlogged private virtio channel.

No provider calls. No keys in arguments, environment, diagnostics or disk images.
The host channel and guest receiver are explicitly prepared by the VM owner.
"""
import argparse
import json
import os
from pathlib import Path
import re
import select
import socket
import stat
import struct
import subprocess
import sys
import tempfile
import time

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.m0_installer.local_secret import normalize_key, read_protected_file

PORT = Path('/dev/virtio-ports/superagent.credential')
DEST = Path('/run/credstore/provider_api_key')
LIMIT = 1024
READY = b'SA_CREDENTIAL_READY_V1 '
OK = b'SA_CREDENTIAL_STORED_V1\n'


def exact(fd, count, deadline):
    data = bytearray()
    while len(data) < count:
        left = deadline - time.monotonic()
        if left <= 0 or not select.select([fd], [], [], left)[0]:
            raise ValueError('credential channel timeout')
        part = os.read(fd, count - len(data))
        if not part: raise ValueError('credential channel closed')
        data.extend(part)
    return bytes(data)


def receive():
    if os.geteuid() != 0: raise ValueError('root receiver required')
    real = PORT.resolve(strict=True)
    if real.parent != Path('/dev') or not re.fullmatch(r'vport[0-9]+p[0-9]+', real.name):
        raise ValueError('unexpected virtio port')
    info = real.stat()
    if not stat.S_ISCHR(info.st_mode) or info.st_uid != 0:
        raise ValueError('unsafe virtio port')
    machine = Path('/etc/machine-id').read_text(encoding='ascii').strip()
    if not re.fullmatch('[0-9a-f]{32}', machine): raise ValueError('invalid machine identity')
    if not DEST.parent.exists(): DEST.parent.mkdir(mode=0o700)
    for path in (Path('/run'), DEST.parent):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise ValueError('unsafe memory credential directory')
    fs = subprocess.run(['/usr/bin/findmnt', '-n', '-o', 'FSTYPE', '--target', str(DEST.parent)],
                        check=True, capture_output=True, timeout=5).stdout.strip()
    if fs != b'tmpfs' or os.path.lexists(DEST): raise ValueError('memory destination unavailable')
    fd = os.open(real, os.O_RDWR | os.O_NOFOLLOW)
    try:
        os.write(fd, READY + machine.encode('ascii') + b'\n')
        deadline = time.monotonic() + 60
        length = struct.unpack('!I', exact(fd, 4, deadline))[0]
        if not 16 <= length <= LIMIT: raise ValueError('credential frame outside bounds')
        key = normalize_key(exact(fd, length, deadline))
        descriptor, tmp = tempfile.mkstemp(prefix='.provider-', dir=DEST.parent)
        try:
            with os.fdopen(descriptor, 'wb') as stream:
                stream.write(key); stream.flush(); os.fsync(stream.fileno())
            os.replace(tmp, DEST)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
        os.write(fd, OK)
    finally:
        os.close(fd)


def send(work, machine, key):
    work = Path(work)
    if work.parent != Path('/var/tmp') or not work.name.startswith('wenet-common-vm.'):
        raise ValueError('unexpected VM work directory')
    info = work.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
        raise ValueError('unsafe VM work directory')
    channel = work / 'credential-input.sock'
    info = channel.lstat()
    if not stat.S_ISSOCK(info.st_mode) or info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o600:
        raise ValueError('unsafe VM credential channel')
    expected = READY + machine.encode('ascii') + b'\n'
    with socket.socket(socket.AF_UNIX) as connection:
        connection.settimeout(60); connection.connect(str(channel))
        if exact(connection.fileno(), len(expected), time.monotonic() + 60) != expected:
            raise ValueError('receiver identity mismatch')
        connection.sendall(struct.pack('!I', len(key)) + key)
        if exact(connection.fileno(), len(OK), time.monotonic() + 10) != OK:
            raise ValueError('receiver did not acknowledge')


def deliver(args):
    if os.name != 'nt': raise ValueError('Windows controller required')
    key = normalize_key(read_protected_file(args.key_file))
    script = Path(__file__).resolve().as_posix()
    linux_path = '/mnt/' + script[0].lower() + script[2:]
    command = ['wsl.exe', '-d', 'Ubuntu', '-u', 'root', '--', '/usr/bin/python3', '-I', '-B',
               linux_path, 'send', '--work', args.work, '--machine', args.machine]
    result = subprocess.run(command, input=key, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            timeout=80, creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode != 0 or result.stdout.strip() != b'{"ok": true, "credential_stored_in_tmpfs": true}':
        raise ValueError('memory delivery failed; inspect fixed receiver status')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('receive', 'send', 'deliver'))
    parser.add_argument('--work')
    parser.add_argument('--machine')
    parser.add_argument('--key-file')
    args = parser.parse_args()
    try:
        if args.command != 'receive' and (not args.work or not re.fullmatch('[0-9a-f]{32}', args.machine or '')):
            raise ValueError('explicit target required')
        if args.command == 'receive': receive()
        elif args.command == 'send': send(args.work, args.machine, normalize_key(sys.stdin.buffer.read(LIMIT + 1)))
        else: deliver(args)
    except Exception:
        print('{"ok":false,"error":"credential_channel_refused"}')
        return 1
    print('{"ok": true, "credential_stored_in_tmpfs": true}')
    return 0


if __name__ == '__main__': raise SystemExit(main())
