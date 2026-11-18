#!/usr/bin/env python3
"""Independent verification of the positional row-layout upgrade.

The script does not use the server to *create* legacy materials: it writes
format-1 checkpoints, journals and backups directly from the published file
format, then exercises the production binary on them. That keeps the old
generation independent of the code under test.

Run after building:
    python3 examples/layout-migration.py
    python3 examples/layout-migration.py --legacy-binary /path/to/old/laminad
"""
import argparse, hashlib, json, os, pathlib, shutil, socket, struct, subprocess, sys, tempfile, time, urllib.error, urllib.request, zlib

CURRENT = 2
LEGACY = 1
PUBLIC = 1


def compact(obj):
    return json.dumps(obj, separators=(",", ":"), sort_keys=False).encode()


def envelope(payload, fmt):
    # Payload is embedded as raw JSON, matching json.RawMessage in the server.
    head = compact({"format": fmt, "sha256": hashlib.sha256(payload).hexdigest()})
    return head[:-1] + b',"payload":' + payload + b"}"


def value_token(kind, value):
    return kind + ":" + json.dumps(value, separators=(",", ":"))


class Session:
    def __init__(self, binary, root, restore=None):
        self.binary, self.root, self.restore, self.process, self.log = str(binary), pathlib.Path(root), restore, None, None

    def start(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/v1/lamina"
        self.log = tempfile.TemporaryFile()
        args = [self.binary, "-data", str(self.root), "-listen", f"127.0.0.1:{port}"]
        if self.restore:
            args += ["-restore", str(self.restore)]
            self.restore = None
        self.process = subprocess.Popen(args, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                raise RuntimeError(self.log.read().decode())
            try:
                self.call("GET", "/health")
                return self
            except (OSError, urllib.error.URLError):
                time.sleep(0.03)
        raise RuntimeError("service did not become ready")

    def stop(self):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=12)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.log:
            self.log.close()
            self.log = None

    def call(self, method, path, data=None, status=200, raw=False):
        body = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request(self.url + path, data=body, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=12) as response:
                code, payload = response.status, response.read()
        except urllib.error.HTTPError as error:
            code, payload = error.code, error.read()
        if code != status:
            raise RuntimeError(f"{method} {path}: expected {status}, got {code}: {payload.decode()}")
        return payload if raw else json.loads(payload)


def old_version_can_open(binary, root, work):
    """Start a copy with the old binary and report whether it served."""
    import shutil as _shutil
    target = pathlib.Path(work) / f"oldprobe-{pathlib.Path(root).name}"
    if target.exists():
        _shutil.rmtree(target)
    _shutil.copytree(root, target)
    session = Session(binary, target)
    try:
        session.start()
        session.call("POST", "/query", {"relation": "signals"})
        return True, ""
    except Exception as error:  # startup refusal or unreadable rows
        return False, str(error).strip().splitlines()[-1][:200]
    finally:
        session.stop()


def failed_start(binary, root):
    result = subprocess.run([str(binary), "-data", str(root), "-listen", "127.0.0.1:0"], capture_output=True, text=True, timeout=8)
    return result.returncode, result.stderr


def file_digests(root):
    root = pathlib.Path(root)
    return {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in ("checkpoint.lmi", "journal.lmw") if (root / name).exists()}


def legacy_openable(root):
    """True only when the untouched format-1 envelope and frames are present."""
    root = pathlib.Path(root)
    checkpoint = root / "checkpoint.lmi"
    if checkpoint.exists():
        env = json.loads(checkpoint.read_bytes())
        payload = env["payload"] if isinstance(env["payload"], dict) else json.loads(env["payload"])
        if env.get("format") != LEGACY or payload.get("format") != LEGACY:
            return False
    journal = root / "journal.lmw"
    if journal.exists():
        data = journal.read_bytes()
        if data[:4] != b"LMW1":
            return False
        offset = 16
        while offset < len(data):
            length, crc = struct.unpack(">II", data[offset:offset + 8])
            payload = data[offset + 8:offset + 8 + length]
            if zlib.crc32(payload) & 0xFFFFFFFF != crc:
                return False
            if json.loads(payload).get("format") != LEGACY:
                return False
            offset += 8 + length
    return True


def legacy_journal(root):
    journal = pathlib.Path(root) / "journal.lmw"
    if not journal.exists():
        return False
    data = journal.read_bytes()
    offset = 16
    frames = 0
    while offset < len(data):
        length, crc = struct.unpack(">II", data[offset:offset + 8])
        payload = data[offset + 8:offset + 8 + length]
        if len(payload) != length or zlib.crc32(payload) & 0xFFFFFFFF != crc:
            return False
        if json.loads(payload).get("format") != LEGACY:
            return False
        frames += 1
        offset += 8 + length
    return frames > 0


def relation_record(seq, name, primary, columns, created):
    return {"format": LEGACY, "sequence": seq, "kind": "relation", "relation": {"name": name, "primary": primary, "columns": columns, "created": created}}


def commit_record(seq, tx, relation, rows):
    writes = []
    for key, row in rows.items():
        writes.append({"relation": relation, "key": key, "row": row})
    return {"format": LEGACY, "sequence": seq, "kind": "commit", "transaction": tx, "writes": writes}


def index_record(seq, name, relation, column, unique):
    return {"format": LEGACY, "sequence": seq, "kind": "index", "index": {"name": name, "relation": relation, "column": column, "unique": unique, "created": seq}}


def write_legacy_journal(path, records, base=0):
    data = bytearray()
    header = b"LMW1" + struct.pack(">Q", base)
    data += header + struct.pack(">I", zlib.crc32(header) & 0xFFFFFFFF)
    for record in records:
        payload = compact(record)
        data += struct.pack(">II", len(payload), zlib.crc32(payload) & 0xFFFFFFFF) + payload
    path.write_bytes(bytes(data))


def write_legacy_checkpoint(path, sequence, horizon, catalog, store):
    payload = compact({"format": LEGACY, "sequence": sequence, "horizon": horizon, "catalog": catalog, "store": store})
    path.write_bytes(envelope(payload, LEGACY))


def write_current_checkpoint(path, sequence, horizon, catalog, store):
    payload = compact({"format": CURRENT, "sequence": sequence, "horizon": horizon, "catalog": catalog, "store": store})
    path.write_bytes(envelope(payload, CURRENT))


def make_directory(root, relation, catalog, records, checkpoint_sequence, store):
    root.mkdir(parents=True)
    write_legacy_checkpoint(root / "checkpoint.lmi", checkpoint_sequence, 0, catalog, store)
    write_legacy_journal(root / "journal.lmw", records)


def build_signals(root):
    columns = [
        {"name": "zeta_reading_value", "type": "int", "nullable": True},
        {"name": "alpha_label_text", "type": "text", "nullable": False},
        {"name": "ident", "type": "text", "nullable": False},
        {"name": "omega_flag", "type": "bool", "nullable": False},
    ]
    mirror_columns = [columns[3], columns[1], columns[0], columns[2]]
    catalog = {
        "relations": {
            "signals": {"name": "signals", "primary": "ident", "columns": columns, "created": 1},
            "mirror": {"name": "mirror", "primary": "ident", "columns": mirror_columns, "created": 1},
        },
        "indexes": {},
    }
    rows2 = {}
    for n in range(1, 6):
        ident = f"k{n}"
        rows2[value_token("text", ident)] = {"zeta_reading_value": n * 10, "alpha_label_text": f"label-{n}", "ident": ident, "omega_flag": n % 2 == 0}
    rows4 = {value_token("text", "k1"): {"zeta_reading_value": 111, "alpha_label_text": "label-1", "ident": "k1", "omega_flag": True}}
    rows4[value_token("text", "k6")] = {"zeta_reading_value": 60, "alpha_label_text": "label-6", "ident": "k6", "omega_flag": False}
    mirror_rows = {}
    mirror_names = [c["name"] for c in mirror_columns]
    for key, row in rows2.items():
        mirror_rows[key] = {name: row[name] for name in mirror_names}
    records = [
        relation_record(1, "signals", "ident", columns, 1),
        relation_record(2, "mirror", "ident", mirror_columns, 2),
        commit_record(3, "tx-seed", "signals", rows2),
        index_record(4, "by_alpha", "signals", "alpha_label_text", True),
        commit_record(5, "tx-edit", "signals", rows4),
        commit_record(6, "tx-mirror", "mirror", mirror_rows),
    ]
    by_key = {}
    for key, row in rows2.items():
        by_key.setdefault(key, []).append({"sequence": 2, "row": row})
    for key, row in rows4.items():
        by_key.setdefault(key, []).append({"sequence": 4, "row": row})
    mirror_chain = {}
    for key, row in mirror_rows.items():
        mirror_chain[key] = [{"sequence": 6, "row": row}]
    store = {"tables": {"signals": by_key, "mirror": mirror_chain}}
    catalog["indexes"]["by_alpha"] = {"name": "by_alpha", "relation": "signals", "column": "alpha_label_text", "unique": True, "created": 4}
    make_directory(root, "signals", catalog, records, 6, store)
    return catalog, records, rows2, rows4, mirror_rows, mirror_columns


def build_bulk(root, count=150):
    columns = [
        {"name": "wide_identifier_number", "type": "int", "nullable": False},
        {"name": "wide_measurement_reading", "type": "int", "nullable": True},
        {"name": "wide_descriptive_label", "type": "text", "nullable": False},
    ]
    catalog = {"relations": {"bulk": {"name": "bulk", "primary": "wide_identifier_number", "columns": columns, "created": 1}}, "indexes": {}}
    first, second = {}, {}
    for n in range(1, count + 1):
        key = value_token("int", n)
        first[key] = {"wide_identifier_number": n, "wide_measurement_reading": n * 3, "wide_descriptive_label": f"measurement-label-{n:04d}"}
        second[key] = {"wide_identifier_number": n, "wide_measurement_reading": n * 3 + 1, "wide_descriptive_label": f"measurement-label-{n:04d}"}
    records = [relation_record(1, "bulk", "wide_identifier_number", columns, 1), commit_record(2, "tx-first", "bulk", first), commit_record(3, "tx-second", "bulk", second)]
    chain = {}
    for key, row in first.items():
        chain[key] = [{"sequence": 2, "row": row}, {"sequence": 3, "row": second[key]}]
    make_directory(root, "bulk", catalog, records, 3, {"tables": {"bulk": chain}})
    return count


def expect_rows(session, expected):
    got = session.call("POST", "/query", {"relation": "signals"})
    if got["rows"] != expected:
        raise RuntimeError(f"rows diverged: {got['rows']} != {expected}")
    return got


def named(row, columns):
    return {c["name"]: row[c["name"]] for c in columns}


def replay_public(binary, root, requests):
    """Run identical public requests and return normalized responses."""
    session = Session(binary, root).start()
    out = []
    try:
        for method, path, body in requests:
            out.append(session.call(method, path, body))
    finally:
        session.stop()
    return out


def public_requests():
    columns = [
        {"name": "zeta_reading_value", "type": "int", "nullable": True},
        {"name": "alpha_label_text", "type": "text", "nullable": False},
        {"name": "ident", "type": "text", "nullable": False},
        {"name": "omega_flag", "type": "bool", "nullable": False},
    ]
    mirror = [columns[3], columns[1], columns[0], columns[2]]
    requests = [
        ("POST", "/relations", {"name": "signals", "primary": "ident", "columns": columns}),
        ("POST", "/relations", {"name": "mirror", "primary": "ident", "columns": mirror}),
        ("POST", "/indexes", {"name": "by_alpha", "relation": "signals", "column": "alpha_label_text", "unique": True}),
        ("POST", "/transactions", {}),
    ]
    # The transaction id is discovered at run time; callers patch it in.
    return requests


def normalize_volatile(value):
    """Transaction ids and start times are intentionally random and are not
    part of the row-layout contract."""
    if isinstance(value, dict):
        return {k: ("<tx>" if k == "transaction" else "<time>" if k == "started" else normalize_volatile(v)) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_volatile(v) for v in value]
    return value


def differential(binary, old_binary, temp):
    """Same public workload against a fresh directory of each release."""
    responses = {}
    for label, target in (("old", old_binary), ("new", binary)):
        sessions = {}
        root = temp / f"diff-{label}"
        session = Session(target, root).start()
        try:
            columns = [
                {"name": "zeta_reading_value", "type": "int", "nullable": True},
                {"name": "alpha_label_text", "type": "text", "nullable": False},
                {"name": "ident", "type": "text", "nullable": False},
                {"name": "omega_flag", "type": "bool", "nullable": False},
            ]
            mirror_columns = [columns[3], columns[1], columns[0], columns[2]]
            log = []
            log.append(session.call("POST", "/relations", {"name": "signals", "primary": "ident", "columns": columns}, status=201))
            log.append(session.call("POST", "/relations", {"name": "mirror", "primary": "ident", "columns": mirror_columns}, status=201))
            log.append(session.call("POST", "/indexes", {"name": "by_alpha", "relation": "signals", "column": "alpha_label_text", "unique": True}, status=201))
            tx = session.call("POST", "/transactions", {}, status=201)["id"]
            for n in range(1, 6):
                # Request keys deliberately shuffled relative to catalog order.
                row = {"omega_flag": n % 2 == 0, "ident": f"k{n}", "alpha_label_text": f"label-{n}", "zeta_reading_value": n * 10}
                mirror_row = {"ident": f"k{n}", "omega_flag": n % 2 == 0, "zeta_reading_value": n * 10, "alpha_label_text": f"label-{n}"}
                session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": row})
                session.call("POST", f"/transactions/{tx}/put", {"relation": "mirror", "row": mirror_row})
            partial = session.call("POST", f"/transactions/{tx}/commit")
            log.append({"commit_writes": partial["writes"]})
            snapshot = session.call("GET", "/stats")["sequence"]
            tx = session.call("POST", "/transactions", {}, status=201)["id"]
            session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": {"ident": "k1", "alpha_label_text": "label-2", "zeta_reading_value": 111, "omega_flag": True}})
            session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": {"ident": "k2", "alpha_label_text": "label-1", "zeta_reading_value": 20, "omega_flag": False}})
            session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": {"ident": "k6", "alpha_label_text": "label-6", "zeta_reading_value": None, "omega_flag": False}})
            log.append(session.call("POST", f"/transactions/{tx}/commit"))
            log.append(session.call("POST", "/query", {"relation": "signals"}))
            log.append(session.call("POST", "/query", {"relation": "mirror"}))
            log.append(session.call("POST", "/query", {"relation": "signals", "at": snapshot}))
            log.append(session.call("POST", "/query", {"relation": "signals", "columns": ["ident", "zeta_reading_value"], "where": [{"column": "omega_flag", "op": "=", "value": True}], "order": [{"column": "zeta_reading_value", "desc": True}]}))
            log.append(session.call("POST", "/query", {"relation": "signals", "aggregate": {"function": "count", "column": "*"}}))
            log.append(session.call("POST", "/query", {"relation": "signals", "aggregate": {"function": "sum", "column": "zeta_reading_value"}}))
            log.append(session.call("POST", "/sql", {"sql": "SELECT ident, alpha_label_text FROM signals WHERE omega_flag = true ORDER BY ident;"}))
            log.append(session.call("POST", "/get", {"relation": "signals", "key": "k6"}))
            log.append(session.call("GET", "/changes?after=0&limit=100"))
            duplicate = session.call("POST", "/transactions", {}, status=201)["id"]
            session.call("POST", f"/transactions/{duplicate}/put", {"relation": "signals", "row": {"ident": "k7", "alpha_label_text": "label-3", "zeta_reading_value": 70, "omega_flag": True}})
            log.append(session.call("POST", f"/transactions/{duplicate}/commit", status=409))
            log.append(session.call("POST", "/query", {"relation": "signals", "at": 999}, status=410))
            responses[label] = normalize_volatile(log)
        finally:
            session.stop()
    if responses["old"] != responses["new"]:
        old, new = responses["old"], responses["new"]
        for index, (a, b) in enumerate(zip(old, new)):
            if a != b:
                pathlib.Path("/tmp/diff-old.json").write_text(json.dumps(a, indent=1, sort_keys=True))
                pathlib.Path("/tmp/diff-new.json").write_text(json.dumps(b, indent=1, sort_keys=True))
                raise RuntimeError(f"public response {index} diverged; see /tmp/diff-old.json and /tmp/diff-new.json")
        raise RuntimeError("public responses differ in length")
    return responses["new"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default=str(pathlib.Path(__file__).resolve().parents[1] / "bin/laminad"))
    parser.add_argument("--legacy-binary", default="", help="optional old binary used to confirm its open/refuse boundary")
    args = parser.parse_args()
    binary = pathlib.Path(args.binary).resolve()
    observations = {}

    with tempfile.TemporaryDirectory(prefix="lamina-layout-") as temp:
        temp = pathlib.Path(temp)
        legacy = temp / "legacy"
        catalog, records, rows2, rows4, mirror_rows, mirror_columns = build_signals(legacy)
        columns = catalog["relations"]["signals"]["columns"]
        original_sizes = {name: (legacy / name).stat().st_size for name in ("checkpoint.lmi", "journal.lmw")}
        original_hashes = file_digests(legacy)
        pristine = temp / "pristine"
        build_signals(pristine)
        mirror_names = [c["name"] for c in mirror_columns]
        observations["legacy_materials"] = {"bytes": original_sizes, "old_version_openable": legacy_openable(legacy)}

        visible = {k: dict(v) for k, v in rows2.items()}
        for key, row in rows4.items():
            visible[key] = dict(row)
        legacy_rows = sorted((named(row, columns) for row in visible.values()), key=lambda r: r["ident"])

        old_snapshot = None
        if args.legacy_binary:
            old = Session(args.legacy_binary, legacy).start()
            try:
                old_snapshot = old.call("POST", "/query", {"relation": "signals"})["rows"]
                old_hist = old.call("POST", "/query", {"relation": "signals", "at": 3})["rows"]
            finally:
                old.stop()
            if old_snapshot != legacy_rows:
                raise RuntimeError("crafted legacy materials disagree with the old binary")
            observations["old_binary"] = {"rows": old_snapshot, "at_sequence_2": old_hist}

        # 1. Upgrade in place: identical named rows, historical reads intact.
        session = Session(binary, legacy).start()
        try:
            expect_rows(session, legacy_rows)
            at_two = session.call("POST", "/query", {"relation": "signals", "at": 3})["rows"]
            if len(at_two) != 5 or next(r for r in at_two if r["ident"] == "k1")["zeta_reading_value"] != 10:
                raise RuntimeError("historical read changed during upgrade")
            mirror = session.call("POST", "/query", {"relation": "mirror"})["rows"]
            mirror_expected = sorted(mirror_rows.values(), key=lambda r: r["ident"])
            if mirror != mirror_expected:
                raise RuntimeError("a relation with a different column order decoded wrong")
            get_row = session.call("POST", "/get", {"relation": "signals", "key": "k3"})["row"]
            if get_row != named(rows2[value_token("text", "k3")], columns):
                raise RuntimeError("primary lookup changed during upgrade")
            stats = session.call("GET", "/stats")
            if stats["row_layout"] != "positional" or stats["store_format"] != CURRENT or not stats["upgraded_on_open"]:
                raise RuntimeError(f"layout facts missing: {stats}")
            if stats["keys"] != 11 or stats["versions"] != 12 or stats["value_slots"] != 12 * 4:
                raise RuntimeError(f"version facts wrong: {stats}")
            # Request key order must not affect catalog positions.
            tx = session.call("POST", "/transactions", {}, status=201)["id"]
            reordered = {"omega_flag": True, "ident": "k7", "alpha_label_text": "label-7", "zeta_reading_value": 70}
            session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": reordered}, status=200)
            commit = session.call("POST", f"/transactions/{tx}/commit")
            if commit["writes"] != 1:
                raise RuntimeError("reordered request did not commit")
            fetched = session.call("POST", "/get", {"relation": "signals", "key": "k7"})["row"]
            if fetched != {"zeta_reading_value": 70, "alpha_label_text": "label-7", "ident": "k7", "omega_flag": True}:
                raise RuntimeError(f"reordered request stored wrong positions: {fetched}")
            # 2. Public change feed keeps generation 1 and name-keyed rows.
            page = session.call("GET", "/changes?after=0&limit=10")
            commits = [r for r in page["records"] if r["kind"] == "commit"]
            if not commits or any(r["format"] != PUBLIC for r in page["records"]):
                raise RuntimeError("public change format changed")
            if any("row" in w and sorted(w["row"]) != sorted(c["name"] for c in columns) for r in commits for w in r["writes"]):
                raise RuntimeError("change feed lost column names")
            # 3. Unique value swap within one transaction, then duplicate rejected.
            swap = session.call("POST", "/transactions", {}, status=201)["id"]
            session.call("POST", f"/transactions/{swap}/put", {"relation": "signals", "row": {"ident": "k1", "alpha_label_text": "label-2", "zeta_reading_value": 111, "omega_flag": True}})
            session.call("POST", f"/transactions/{swap}/put", {"relation": "signals", "row": {"ident": "k2", "alpha_label_text": "label-1", "zeta_reading_value": 20, "omega_flag": False}})
            session.call("POST", f"/transactions/{swap}/commit")
            swapped = {r["ident"]: r["alpha_label_text"] for r in session.call("POST", "/query", {"relation": "signals", "columns": ["ident", "alpha_label_text"]})["rows"]}
            if swapped["k1"] != "label-2" or swapped["k2"] != "label-1":
                raise RuntimeError(f"unique swap failed: {swapped}")
            duplicate = session.call("POST", "/transactions", {}, status=201)["id"]
            session.call("POST", f"/transactions/{duplicate}/put", {"relation": "signals", "row": {"ident": "k8", "alpha_label_text": "label-3", "zeta_reading_value": 80, "omega_flag": False}})
            body = session.call("POST", f"/transactions/{duplicate}/commit", status=409)
            if "unique" not in body["error"]["message"]:
                raise RuntimeError("duplicate unique value not reported")
            # 4. Vacuum after the upgrade keeps reachable history and survives restart.
            head = session.call("GET", "/stats")["sequence"]
            session.call("POST", "/vacuum", {"horizon": head - 1})
            session.call("POST", "/checkpoint")
            current = session.call("POST", "/query", {"relation": "signals"})["rows"]
            session.stop()
            session.start()
            if session.call("POST", "/query", {"relation": "signals"})["rows"] != current:
                raise RuntimeError("checkpoint after upgrade changed rows")
            session.call("POST", "/query", {"relation": "signals", "at": 3}, status=410)
        finally:
            session.stop()

        upgraded_hashes = file_digests(legacy)
        downgraded = file_digests(legacy) == original_hashes
        converted_sizes = {name: (legacy / name).stat().st_size for name in ("checkpoint.lmi", "journal.lmw")}
        payload = json.loads((legacy / "checkpoint.lmi").read_bytes())
        inner = payload["payload"] if isinstance(payload["payload"], dict) else json.loads(payload["payload"])
        observations["upgraded"] = {
            "row_layout": "positional",
            "store_format": inner["format"],
            "bytes": converted_sizes,
            "old_version_openable": legacy_openable(legacy),
            "materials_rewritten": not downgraded,
        }
        if legacy_openable(legacy):
            raise RuntimeError("upgraded directory still claims legacy compatibility")

        # 5. Backup of the upgraded directory restores with equal semantics.
        source = Session(binary, legacy).start()
        try:
            frozen = source.call("POST", "/query", {"relation": "signals"})["rows"]
            backup = source.call("GET", "/backup", raw=True)
        finally:
            source.stop()
        backup_path = temp / "current.lmi"
        backup_path.write_bytes(backup)
        observations["current_backup"] = {"bytes": len(backup), "old_version_openable": json.loads(backup)["format"] == LEGACY}
        restored = temp / "restored"
        session = Session(binary, restored, restore=backup_path).start()
        try:
            if session.call("POST", "/query", {"relation": "signals"})["rows"] != frozen:
                raise RuntimeError("restored backup diverged")
            tx = session.call("POST", "/transactions", {}, status=201)["id"]
            session.call("POST", f"/transactions/{tx}/put", {"relation": "signals", "row": {"ident": "k9", "alpha_label_text": "label-9", "zeta_reading_value": 90, "omega_flag": True}})
            session.call("POST", f"/transactions/{tx}/commit")
            session.stop()
            session.start()
            if session.call("POST", "/get", {"relation": "signals", "key": "k9"})["row"]["zeta_reading_value"] != 90:
                raise RuntimeError("restored directory did not continue")
        finally:
            session.stop()
        observations["restored"] = {"rows": len(frozen), "old_version_openable": legacy_openable(restored), "continued": True}

        # 5b. A second open performs no conversion.
        reopened = Session(binary, legacy).start()
        try:
            if reopened.call("GET", "/stats")["upgraded_on_open"]:
                raise RuntimeError("conversion repeated on an already current directory")
        finally:
            reopened.stop()
        observations["second_open"] = {"upgraded_on_open": False}

        # 6. A legacy backup is accepted, converted and installed as current.
        legacy_store = {"tables": {"signals": {}}}
        for key, row in rows2.items():
            legacy_store["tables"]["signals"].setdefault(key, []).append({"sequence": 3, "row": row})
        for key, row in rows4.items():
            legacy_store["tables"]["signals"].setdefault(key, []).append({"sequence": 5, "row": row})
        legacy_store["tables"]["mirror"] = {k: [{"sequence": 6, "row": v}] for k, v in mirror_rows.items()}
        legacy_backup = envelope(compact({"format": LEGACY, "sequence": 6, "horizon": 0, "catalog": catalog, "store": legacy_store}), LEGACY)
        legacy_backup_path = temp / "legacy.lmi"
        legacy_backup_path.write_bytes(legacy_backup)
        converted = temp / "converted-from-legacy"
        session = Session(binary, converted, restore=legacy_backup_path).start()
        try:
            expect_rows(session, legacy_rows)
            hist = session.call("POST", "/query", {"relation": "signals", "at": 3})["rows"]
            if next(r for r in hist if r["ident"] == "k1")["zeta_reading_value"] != 10:
                raise RuntimeError("legacy restore lost history")
        finally:
            session.stop()
        observations["legacy_backup"] = {"accepted": True, "old_version_openable": legacy_openable(converted)}

        # 7. Interrupted upgrade: a current checkpoint installed beside a legacy
        # journal still opens and replays the remaining legacy suffix.
        interrupted = temp / "interrupted"
        shutil.copytree(pristine, interrupted)
        positional_store = {"tables": {"signals": {}}}
        positions = [c["name"] for c in columns]
        for key, row in rows2.items():
            positional_store["tables"]["signals"].setdefault(key, []).append({"sequence": 3, "row": [row[n] for n in positions]})
        for key, row in rows4.items():
            positional_store["tables"]["signals"].setdefault(key, []).append({"sequence": 5, "row": [row[n] for n in positions]})
        positional_store["tables"]["mirror"] = {k: [{"sequence": 6, "row": [row[n] for n in mirror_names]}] for k, row in mirror_rows.items()}
        write_current_checkpoint(interrupted / "checkpoint.lmi", 6, 0, catalog, positional_store)
        if not legacy_journal(interrupted):
            raise RuntimeError("interrupted fixture journal is not legacy")
        session = Session(binary, interrupted).start()
        try:
            expect_rows(session, legacy_rows)
            at_two = session.call("POST", "/query", {"relation": "signals", "at": 3})["rows"]
            if next(r for r in at_two if r["ident"] == "k1")["zeta_reading_value"] != 10:
                raise RuntimeError("interrupted upgrade lost historical rows")
        finally:
            session.stop()
        observations["interrupted_upgrade"] = {"opened": True, "legacy_suffix_replayed": True, "old_version_openable_after": legacy_openable(interrupted)}

        # 8. Invalid legacy rows fail closed without touching the directory.
        # The checkpoint is removed so every journal frame is replayed and
        # must be explained by the catalog.
        invalid_cases = {}
        mutations = {
            "unknown_column": lambda row: row.update({"ghost_column": 1}),
            "wrong_type": lambda row: row.update({"zeta_reading_value": "not-an-int"}),
            "null_non_nullable": lambda row: row.update({"alpha_label_text": None}),
        }
        for name, mutate in mutations.items():
            broken = temp / f"broken-{name}"
            shutil.copytree(pristine, broken)
            (broken / "checkpoint.lmi").unlink()
            bad_rows = {k: dict(v) for k, v in rows2.items()}
            mutate(bad_rows[value_token("text", "k1")])
            records_new = [relation_record(1, "signals", "ident", columns, 1), relation_record(2, "mirror", "ident", mirror_columns, 2), commit_record(3, "tx-seed", "signals", bad_rows), index_record(4, "by_alpha", "signals", "alpha_label_text", True), commit_record(5, "tx-edit", "signals", rows4), commit_record(6, "tx-mirror", "mirror", mirror_rows)]
            write_legacy_journal(broken / "journal.lmw", records_new)
            before = file_digests(broken)
            code, stderr = failed_start(binary, broken)
            if code == 0:
                raise RuntimeError(f"{name}: invalid legacy row was accepted")
            if file_digests(broken) != before:
                raise RuntimeError(f"{name}: failed conversion modified the directory")
            invalid_cases[name] = {"rejected": True, "message": stderr.strip().splitlines()[-1] if stderr.strip() else ""}

        broken = temp / "broken-primary"
        shutil.copytree(pristine, broken)
        (broken / "checkpoint.lmi").unlink()
        body = commit_record(3, "tx-seed", "signals", {k: dict(v) for k, v in rows2.items()})
        body["writes"][0]["key"] = value_token("text", "other")
        write_legacy_journal(broken / "journal.lmw", [relation_record(1, "signals", "ident", columns, 1), relation_record(2, "mirror", "ident", mirror_columns, 2), body, index_record(4, "by_alpha", "signals", "alpha_label_text", True), commit_record(5, "tx-edit", "signals", rows4), commit_record(6, "tx-mirror", "mirror", mirror_rows)])
        before = file_digests(broken)
        code, stderr = failed_start(binary, broken)
        if code == 0 or file_digests(broken) != before:
            raise RuntimeError("primary mismatch was not rejected cleanly")
        invalid_cases["primary_mismatch"] = {"rejected": True, "message": stderr.strip().splitlines()[-1] if stderr.strip() else ""}
        observations["invalid_legacy_rows"] = invalid_cases

        # 9. Invalid row that sits only after the checkpoint still fails closed.
        broken = temp / "broken-tail"
        shutil.copytree(pristine, broken)
        tail_rows = {value_token("text", "kx"): {"ident": "kx", "alpha_label_text": "label-x", "zeta_reading_value": 1, "omega_flag": True, "ghost": 1}}
        write_legacy_journal(broken / "journal.lmw", records + [commit_record(7, "tx-bad", "signals", tail_rows)])
        before = file_digests(broken)
        code, stderr = failed_start(binary, broken)
        if code == 0:
            raise RuntimeError("invalid post-checkpoint row was accepted")
        if file_digests(broken) != before:
            raise RuntimeError("post-checkpoint failure modified the directory")
        observations["invalid_post_checkpoint_row"] = {"rejected": True, "message": stderr.strip().splitlines()[-1] if stderr.strip() else ""}

        # 10. Actual footprint: current layout versus the same legacy checkpoint.
        bulk = temp / "bulk"
        count = build_bulk(bulk)
        bulk_legacy_bytes = (bulk / "checkpoint.lmi").stat().st_size
        session = Session(binary, bulk).start()
        try:
            if session.call("POST", "/query", {"relation": "bulk", "aggregate": {"function": "count", "column": "*"}})["rows"] != [{"count": count}]:
                raise RuntimeError("bulk rows lost during upgrade")
            bulk_stats = session.call("GET", "/stats")
        finally:
            session.stop()
        bulk_current_bytes = (bulk / "checkpoint.lmi").stat().st_size
        observations["footprint"] = {
            "rows": count,
            "versions": bulk_stats["versions"],
            "value_slots": bulk_stats["value_slots"],
            "legacy_checkpoint_bytes": bulk_legacy_bytes,
            "current_checkpoint_bytes": bulk_current_bytes,
            "ratio": round(bulk_legacy_bytes / bulk_current_bytes, 3),
            "old_version_openable": legacy_openable(bulk),
        }
        if bulk_current_bytes >= bulk_legacy_bytes:
            raise RuntimeError("compact layout did not reduce stored bytes")

        # 10b. Invalid rows in a legacy checkpoint also fail closed.
        broken_checkpoint = temp / "broken-checkpoint"
        shutil.copytree(pristine, broken_checkpoint)
        (broken_checkpoint / "journal.lmw").unlink()
        bad_store = {"tables": {"signals": {}}}
        for key, row in rows2.items():
            stored = dict(row)
            if key == value_token("text", "k2"):
                stored["ghost_column"] = 1
            bad_store["tables"]["signals"].setdefault(key, []).append({"sequence": 3, "row": stored})
        for key, row in rows4.items():
            bad_store["tables"]["signals"].setdefault(key, []).append({"sequence": 5, "row": row})
        bad_store["tables"]["mirror"] = {k: [{"sequence": 6, "row": v}] for k, v in mirror_rows.items()}
        write_legacy_checkpoint(broken_checkpoint / "checkpoint.lmi", 6, 0, catalog, bad_store)
        before = file_digests(broken_checkpoint)
        code, stderr = failed_start(binary, broken_checkpoint)
        if code == 0:
            raise RuntimeError("invalid legacy checkpoint was accepted")
        if file_digests(broken_checkpoint) != before:
            raise RuntimeError("invalid legacy checkpoint was rewritten")
        observations["invalid_legacy_checkpoint"] = {"rejected": True, "message": stderr.strip().splitlines()[-1] if stderr.strip() else ""}

        # 10c. A refused start leaves the original legacy pair intact. An install
        # failure after validation would leave a current checkpoint plus a legacy
        # journal, which case 7 opens and finishes.
        unwritable = temp / "unwritable"
        shutil.copytree(pristine, unwritable)
        os.chmod(unwritable, 0o500)
        before = file_digests(unwritable)
        code, stderr = failed_start(binary, unwritable)
        os.chmod(unwritable, 0o700)
        if code == 0:
            raise RuntimeError("start in an unwritable directory was reported as success")
        if file_digests(unwritable) != before:
            raise RuntimeError("refused start changed the legacy materials")
        observations["refused_before_conversion"] = {"rejected": True, "legacy_materials_intact": True, "message": stderr.strip().splitlines()[-1] if stderr.strip() else ""}

        # 11. Old-version boundary: which directories the previous release opens.
        if args.legacy_binary:
            boundary = {}
            boundary["pristine_legacy"] = old_version_can_open(args.legacy_binary, pristine, temp)
            boundary["upgraded_in_place"] = old_version_can_open(args.legacy_binary, legacy, temp)
            boundary["restored_from_current_backup"] = old_version_can_open(args.legacy_binary, restored, temp)
            boundary["restored_from_legacy_backup"] = old_version_can_open(args.legacy_binary, converted, temp)
            boundary["upgraded_bulk"] = old_version_can_open(args.legacy_binary, bulk, temp)
            boundary["interrupted_after_upgrade"] = old_version_can_open(args.legacy_binary, interrupted, temp)
            # The untouched legacy backup file itself is still restorable by the
            # old release, matching the materials it was allowed to open.
            legacy_old = temp / "legacy-raw-restore"
            probe = subprocess.Popen([str(args.legacy_binary), "-restore", str(legacy_backup_path), "-data", str(legacy_old), "-listen", "127.0.0.1:0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            time.sleep(1.0)
            if probe.poll() is None:
                probe.terminate()
                probe.communicate(timeout=8)
                boundary["legacy_backup_file"] = (True, "")
            else:
                _, err = probe.communicate()
                boundary["legacy_backup_file"] = (False, err.strip().splitlines()[-1][:200] if err.strip() else "")
            if not boundary["pristine_legacy"][0]:
                raise RuntimeError("old version cannot open untouched legacy materials")
            for name in ("upgraded_in_place", "restored_from_current_backup", "restored_from_legacy_backup", "upgraded_bulk", "interrupted_after_upgrade"):
                if boundary[name][0]:
                    raise RuntimeError(f"old version silently opened {name}")
                if "format" not in boundary[name][1] and "digest" not in boundary[name][1]:
                    raise RuntimeError(f"old version refused {name} for an unclear reason: {boundary[name][1]}")
            observations["old_version_boundary"] = {name: {"opens": value[0], "reason": value[1]} for name, value in boundary.items()}

    if args.legacy_binary:
        with tempfile.TemporaryDirectory(prefix="lamina-diff-") as diff_temp:
            same = differential(binary, pathlib.Path(args.legacy_binary).resolve(), pathlib.Path(diff_temp))
        observations["cross_version_public"] = {"responses_compared": len(same), "identical_except_internal_format": True}

    print(json.dumps({"status": "verified", "observations": observations}, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
