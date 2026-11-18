#!/usr/bin/env python3
"""Build isolated consumers and run local examples; always clean up owned processes."""
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
ENV = {**os.environ, "GOWORK": "off"}
# Reuse the toolchain selected by this module. A new consumer module has no Go
# requirement yet, so auto selection could otherwise query the network again.
ENV["GOTOOLCHAIN"] = subprocess.check_output(
    ["go", "env", "GOVERSION"], cwd=ROOT, env=ENV, text=True).strip()


def run(args, cwd, timeout=240):
    result = subprocess.run(args, cwd=cwd, env=ENV, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise AssertionError(f"{args}:\n{result.stdout}")
    return result.stdout


def address():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def until(check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.05)
    raise AssertionError("timed out waiting for example readiness/state")


class Processes:
    def __init__(self, directory):
        self.directory, self.owned = directory, []

    def start(self, name, args, ready=None):
        log = self.directory / f"{name}.log"
        with log.open("w") as output:
            process = subprocess.Popen(args, cwd=self.directory, env=ENV, stdout=output,
                                       stderr=subprocess.STDOUT, text=True)
        self.owned.append((process, log))
        if ready:
            def check():
                if process.poll() is not None:
                    raise AssertionError(log.read_text())
                return ready in log.read_text()
            until(check)
        return process

    @staticmethod
    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def close(self):
        for process, _ in reversed(self.owned):
            self.stop(process)


def build(example, directory):
    directory.mkdir()
    source = ROOT / "examples" / example
    assert (source / "main.go").exists(), f"missing example: {source}"
    for path in source.glob("*.go"):
        if not path.name.endswith("_test.go"):
            shutil.copy2(path, directory)
    run(["go", "mod", "init", "example.org/" + example], directory)
    run(["go", "mod", "edit", "-require=github.com/fastabc/fastmesh@v0.0.0",
         "-replace=github.com/fastabc/fastmesh=" + str(ROOT)], directory)
    run(["go", "mod", "tidy"], directory)
    deps = run(["go", "list", "-deps", "."], directory).splitlines()
    assert not any(p.startswith("go.etcd.io/") or p == "github.com/fastabc/fastmesh/store/etcd"
                   for p in deps), f"{example} unexpectedly imports the etcd backend"
    run(["go", "build", "-o", "demo", "."], directory)
    return str(directory / "demo")


def provision(binary, directory, flag, roles):
    staging = directory / "provision"
    run([binary, "-role", "setup", flag, str(staging)], directory)
    # Move each bundle out of the provisioning tree. No runtime may depend on it.
    for role in roles:
        bundle = staging / role
        assert sorted(p.name for p in bundle.glob("*-key.pem")) == [role + "-key.pem"]
        shutil.move(str(bundle), str(directory / role))
    staging.rmdir()


def deliver_token(directory, roles):
    for role in roles:
        shutil.copy2(directory / "controller" / "join-token", directory / role / "join-token")


def orders(directory):
    binary = build("orders-payments", directory)
    controller = address()
    base = [binary, "-controller", controller]
    provision(binary, directory, "-state", ["controller", "payments", "orders"])
    processes = Processes(directory)
    try:
        processes.start("controller", base + ["-role", "controller", "-state", str(directory / "controller")], "controller-ready")
        deliver_token(directory, ["payments", "orders"])
        processes.start("payments", base + ["-role", "payments", "-state", str(directory / "payments"),
                           "-listen", "0.0.0.0:0", "-advertise", "127.0.0.1:0"], "payments-ready")
        assert run(base + ["-role", "orders", "-state", str(directory / "orders")], directory, 20).strip() == "paid"
        for path in ["/pay/../admin", "/pay/%2e%2e/admin", "/pay/%2E%2e/admin", "/pay//x", "/payment"]:
            result = subprocess.run(base + ["-role", "orders", "-state", str(directory / "orders"), "-path", path], cwd=directory,
                                    env=ENV, text=True, stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT, timeout=20)
            assert result.returncode != 0 and "payments status: 403" in result.stdout, (path, result.stdout)
        print("orders-payments: isolated module, authenticated HTTP and L7 path denials passed", flush=True)
    finally:
        processes.close()


def config(directory):
    binary = build("app-config", directory)
    control, service, app1, app2 = address(), address(), address(), address()
    base = [binary, "-controller", control]
    controller_args = base + ["-role", "controller", "-dir", str(directory / "controller")]
    provider_args = base + ["-role", "config-server", "-dir", str(directory / "config-server"),
                            "-config-listen", service, "-advertise", service]
    provision(binary, directory, "-dir", ["controller", "config-server", "app-1", "app-2"])
    processes = Processes(directory)

    def revision(addr):
        try:
            with urllib.request.urlopen("http://" + addr + "/config", timeout=1) as response:
                return json.load(response).get("revision")
        except (OSError, ValueError):
            return None

    try:
        controller = processes.start("controller", controller_args, "controller-ready")
        deliver_token(directory, ["config-server", "app-1", "app-2"])
        provider = processes.start("config-server", provider_args, "config-server-ready")
        for name, addr in [("app-1", app1), ("app-2", app2)]:
            processes.start(name, base + ["-role", "app", "-name", name, "-dir", str(directory / name), "-http", addr, "-poll", "200ms"])
        until(lambda: revision(app1) == revision(app2) == 1)
        assert not (directory / "controller" / "state").exists()
        for role in ["config-server", "app-1", "app-2"]:
            assert (directory / role / "state").is_dir()
        path = directory / "config-server" / "app.json"
        replacement = path.with_suffix(".next")
        replacement.write_text(json.dumps({"revision": 2, "greeting": "updated", "checkout_enabled": False}))
        replacement.replace(path)
        until(lambda: revision(app1) == revision(app2) == 2)
        path.write_text("{invalid")
        time.sleep(0.8)
        assert revision(app1) == revision(app2) == 2
        processes.stop(provider)
        processes.stop(controller)
        assert revision(app1) == revision(app2) == 2
        path.write_text(json.dumps({"revision": 3, "greeting": "recovered", "checkout_enabled": True}))
        processes.start("controller-restarted", controller_args, "controller-ready")
        processes.start("config-server-restarted", provider_args, "config-server-ready")
        until(lambda: revision(app1) == revision(app2) == 3)
        print("app-config: four isolated bundles, hot update, invalid input, disconnect and recovery passed", flush=True)
    finally:
        processes.close()


def grpc(directory):
    binary = build("grpc", directory)
    base = [binary, "-controller", address()]
    provision(binary, directory, "-state", ["controller", "payments", "orders"])
    processes = Processes(directory)
    try:
        processes.start("controller", base + ["-role", "controller", "-state", str(directory / "controller")], "controller-ready")
        deliver_token(directory, ["payments", "orders"])
        processes.start("payments", base + ["-role", "payments", "-state", str(directory / "payments"),
                           "-listen", "0.0.0.0:0", "-advertise", "127.0.0.1:0"], "payments-ready")
        output = run(base + ["-role", "orders", "-state", str(directory / "orders")], directory, 20)
        assert "health: SERVING" in output and "watch: PermissionDenied" in output, output
        print("grpc: isolated module, allowed unary and denied streaming method passed", flush=True)
    finally:
        processes.close()


def main():
    with tempfile.TemporaryDirectory(prefix="fastmesh-smoke-") as temp:
        grpc(Path(temp) / "grpc")
        orders(Path(temp) / "orders")
        config(Path(temp) / "config")


if __name__ == "__main__":
    main()
