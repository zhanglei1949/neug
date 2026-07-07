"""Load LDBC SNB CSV data into a NeuG database.

Handles schema creation, creationDate preprocessing for hasCreator edges,
and bulk COPY of all vertex/edge tables in one step.

Usage:
    python3 tools/loader.py --db-dir /path/to/db --data-root /path/to/social_network
"""

import argparse
import resource
import time
import threading
from pathlib import Path
from typing import List, Optional, Tuple
import sys

import duckdb
import neug


def format_bytes(num_bytes: Optional[int]) -> str:
    if num_bytes is None:
        return "unknown"
    value = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024.0 or unit == "TiB":
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{value:.2f} TiB"


def process_peak_rss_bytes() -> Optional[int]:
    """Return process peak RSS from getrusage."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if peak <= 0:
        return None
    if sys.platform == "darwin":
        return int(peak)
    return int(peak) * 1024


def current_rss_bytes() -> Optional[int]:
    """Return current RSS on Linux. Used for resettable phase peak sampling."""
    status_path = Path("/proc/self/status")
    if not status_path.exists():
        return None
    try:
        with status_path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
    except (OSError, ValueError):
        return None
    return None


class PeakRssTracker:
    def __init__(self, sample_interval_s: float = 1.0) -> None:
        self.sample_interval_s = sample_interval_s
        self._peak_bytes: Optional[int] = None
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._sample()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._sample()
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()
        self._sample()

    def peak_bytes(self) -> Optional[int]:
        return self._peak_bytes

    def _run(self) -> None:
        while not self._stop_event.wait(self.sample_interval_s):
            self._sample()

    def _sample(self) -> None:
        rss = current_rss_bytes()
        if rss is None:
            return
        if self._peak_bytes is None or rss > self._peak_bytes:
            self._peak_bytes = rss


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load LDBC SNB CSV data into a NeuG database",
    )
    parser.add_argument(
        "--db-dir",
        type=Path,
        required=True,
        help="Output database directory (will be created if not exists)",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        required=True,
        help="Root directory of SNB CSV data (the social_network/ directory)",
    )
    parser.add_argument(
        "--derived-dir",
        type=str,
        default="derived",
        help="Directory for intermediate CSV files, relative to --data-root or absolute (default: derived)",
    )
    parser.add_argument(
        "--memory-sample-interval",
        type=float,
        default=1.0,
        help="Seconds between RSS samples for phase peak memory reporting (default: 1.0)",
    )
    return parser.parse_args()


def ensure_hascreator_with_date_files(
    data_root: Path, derived_dir: Path
) -> Tuple[Path, Path]:
    """Merge creationDate from message tables into hasCreator edge files."""
    out_comment = derived_dir / "comment_hasCreator_person_with_date.csv"
    out_post = derived_dir / "post_hasCreator_person_with_date.csv"
    if out_comment.exists() and out_post.exists():
        print(f"  Derived files already exist, skipping: {derived_dir}")
        return out_comment, out_post

    con = duckdb.connect(database=":memory:", read_only=False)
    dynamic = data_root / "dynamic"

    print("  Merging creationDate into comment hasCreator ...")
    con.execute(
        f"""COPY (
            SELECT cr."Comment.id" AS "from", cr."Person.id" AS "to", c.creationDate
            FROM read_csv_auto('{dynamic}/comment_hasCreator_person_0_0.csv', delim='|') cr
            LEFT JOIN read_csv_auto('{dynamic}/comment_0_0.csv', delim='|') c
            ON cr."Comment.id" = c.id
        ) TO '{out_comment}' (FORMAT CSV, DELIMITER '|', HEADER)"""
    )

    print("  Merging creationDate into post hasCreator ...")
    con.execute(
        f"""COPY (
            SELECT cr."Post.id" AS "from", cr."Person.id" AS "to", p.creationDate
            FROM read_csv_auto('{dynamic}/post_hasCreator_person_0_0.csv', delim='|') cr
            LEFT JOIN read_csv_auto('{dynamic}/post_0_0.csv', delim='|') p
            ON cr."Post.id" = p.id
        ) TO '{out_post}' (FORMAT CSV, DELIMITER '|', HEADER)"""
    )

    con.close()
    return out_comment, out_post


def reorder_messages(data_root: Path, derived_dir: Path) -> Tuple[Path, Path]:
    """Reorder comment and post CSVs by creator Person.id for data locality."""
    out_comment = derived_dir / "comment_reordered.csv"
    out_post = derived_dir / "post_reordered.csv"
    if out_comment.exists() and out_post.exists():
        print(f"  Reordered files already exist, skipping: {derived_dir}")
        return out_comment, out_post

    con = duckdb.connect(database=":memory:", read_only=False)

    dynamic = data_root / "dynamic"

    print("  Reordering comments by creator Person.id ...")
    con.execute(
        f"""CREATE TABLE comment AS SELECT * FROM read_csv_auto(
            '{dynamic}/comment_0_0.csv', delim='|',
            columns={{'id': 'BIGINT', 'creationDate': 'VARCHAR',
                      'locationIP': 'VARCHAR', 'browserUsed': 'VARCHAR',
                      'content': 'VARCHAR', 'length': 'INTEGER'}})"""
    )
    con.execute(
        f"""CREATE TABLE comment_creator AS SELECT * FROM read_csv_auto(
            '{dynamic}/comment_hasCreator_person_0_0.csv', delim='|')"""
    )
    con.execute(
        f"""COPY (
            SELECT c.id, c.creationDate, c.locationIP, c.browserUsed,
                   c.content, c.length
            FROM comment_creator cr
            LEFT JOIN comment c ON cr."Comment.id" = c.id
            ORDER BY cr."Person.id"
        ) TO '{out_comment}' (FORMAT CSV, DELIMITER '|', HEADER)"""
    )

    print("  Reordering posts by creator Person.id ...")
    con.execute(
        f"""CREATE TABLE post AS SELECT * FROM read_csv_auto(
            '{dynamic}/post_0_0.csv', delim='|',
            columns={{'id': 'BIGINT', 'imageFile': 'VARCHAR',
                      'creationDate': 'VARCHAR', 'locationIP': 'VARCHAR',
                      'browserUsed': 'VARCHAR', 'language': 'VARCHAR',
                      'content': 'VARCHAR', 'length': 'INTEGER'}})"""
    )
    con.execute(
        f"""CREATE TABLE post_creator AS SELECT * FROM read_csv_auto(
            '{dynamic}/post_hasCreator_person_0_0.csv', delim='|')"""
    )
    con.execute(
        f"""COPY (
            SELECT p.id, p.imageFile, p.creationDate, p.locationIP,
                   p.browserUsed, p.language, p.content, p.length
            FROM post_creator cr
            LEFT JOIN post p ON cr."Post.id" = p.id
            ORDER BY cr."Person.id"
        ) TO '{out_post}' (FORMAT CSV, DELIMITER '|', HEADER)"""
    )

    con.close()
    return out_comment, out_post


def build_statements(data_root: Path, derived_dir: Path) -> Tuple[List[str], List[str]]:
    """Return (DDL statements, COPY statements) for the full LDBC SNB schema."""
    hc_comment, hc_post = ensure_hascreator_with_date_files(data_root, derived_dir)

    ddl = [
        # "CREATE NODE TABLE PLACE (id INT64, name VARCHAR(256), url VARCHAR(256), type VARCHAR(32), PRIMARY KEY(id))",
        "CREATE NODE TABLE PERSON (id INT64, firstName VARCHAR(80), lastName VARCHAR(80), gender VARCHAR(80), birthday DATE, creationDate TIMESTAMP, locationIP VARCHAR(80), browserUsed VARCHAR(80), language VARCHAR(256), email VARCHAR(2144), PRIMARY KEY(id))",
        "CREATE NODE TABLE COMMENT (id INT64, creationDate TIMESTAMP, locationIP VARCHAR(80), browserUsed VARCHAR(80), content VARCHAR(2144), length INT32, PRIMARY KEY(id))",
        "CREATE NODE TABLE POST (id INT64, imageFile VARCHAR(80), creationDate TIMESTAMP, locationIP VARCHAR(80), browserUsed VARCHAR(80), language VARCHAR(80), content VARCHAR(2144), length INT32, PRIMARY KEY(id))",
        # "CREATE NODE TABLE FORUM (id INT64, title VARCHAR(256), creationDate TIMESTAMP, PRIMARY KEY(id))",
        # "CREATE NODE TABLE ORGANISATION (id INT64, type VARCHAR(32), name VARCHAR(256), url VARCHAR(256), PRIMARY KEY(id))",
        # "CREATE NODE TABLE TAGCLASS (id INT64, name VARCHAR(256), url VARCHAR(256), PRIMARY KEY(id))",
        # "CREATE NODE TABLE TAG (id INT64, name VARCHAR(256), url VARCHAR(256), PRIMARY KEY(id))",
        "CREATE REL TABLE HASCREATOR (FROM COMMENT TO PERSON, FROM POST TO PERSON, creationDate TIMESTAMP, MANY_TO_ONE) WITH (sort_key_for_nbr='creationDate')",
        # "CREATE REL TABLE HASTAG (FROM POST TO TAG, FROM FORUM TO TAG, FROM COMMENT TO TAG)",
        "CREATE REL TABLE REPLYOF (FROM COMMENT TO COMMENT, FROM COMMENT TO POST, MANY_TO_ONE)",
        # "CREATE REL TABLE CONTAINEROF (FROM FORUM TO POST)",
        # "CREATE REL TABLE HASMEMBER (FROM FORUM TO PERSON, joinDate TIMESTAMP) WITH (sort_key_for_nbr='joinDate')",
        # "CREATE REL TABLE HASMODERATOR (FROM FORUM TO PERSON, MANY_TO_ONE)",
        # "CREATE REL TABLE HASINTEREST (FROM PERSON TO TAG)",
        # "CREATE REL TABLE ISLOCATEDIN (FROM COMMENT TO PLACE, FROM PERSON TO PLACE, FROM POST TO PLACE, FROM ORGANISATION TO PLACE, MANY_TO_ONE)",
        # "CREATE REL TABLE KNOWS (FROM PERSON TO PERSON, creationDate TIMESTAMP)",
        "CREATE REL TABLE LIKES (FROM PERSON TO COMMENT, FROM PERSON TO POST, creationDate TIMESTAMP)",
        # "CREATE REL TABLE WORKAT (FROM PERSON TO ORGANISATION, workFrom INT32)",
        # "CREATE REL TABLE ISPARTOF (FROM PLACE TO PLACE, MANY_TO_ONE)",
        # "CREATE REL TABLE HASTYPE (FROM TAG TO TAGCLASS, MANY_TO_ONE)",
        # "CREATE REL TABLE ISSUBCLASSOF (FROM TAGCLASS TO TAGCLASS, MANY_TO_ONE)",
        # "CREATE REL TABLE STUDYAT (FROM PERSON TO ORGANISATION, classYear INT32)",
    ]

    def f(relpath: str) -> str:
        return str(data_root / relpath).replace("'", "\\'")

    copy = [
        # f"COPY PLACE FROM '{f('static/place_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY PERSON FROM '{f('dynamic/person_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY POST FROM '{f('dynamic/post_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY COMMENT FROM '{f('dynamic/comment_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY FORUM FROM '{f('dynamic/forum_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ORGANISATION FROM '{f('static/organisation_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY TAGCLASS FROM '{f('static/tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY TAG FROM '{f('static/tag_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY HASCREATOR FROM '{hc_comment}' (FROM='COMMENT', TO='PERSON', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY HASCREATOR FROM '{hc_post}' (FROM='POST', TO='PERSON', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASTAG FROM '{f('dynamic/post_hasTag_tag_0_0.csv')}' (FROM='POST', TO='TAG', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASTAG FROM '{f('dynamic/comment_hasTag_tag_0_0.csv')}' (FROM='COMMENT', TO='TAG', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASTAG FROM '{f('dynamic/forum_hasTag_tag_0_0.csv')}' (FROM='FORUM', TO='TAG', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY REPLYOF FROM '{f('dynamic/comment_replyOf_comment_0_0.csv')}' (FROM='COMMENT', TO='COMMENT', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY REPLYOF FROM '{f('dynamic/comment_replyOf_post_0_0.csv')}' (FROM='COMMENT', TO='POST', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY CONTAINEROF FROM '{f('dynamic/forum_containerOf_post_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASMEMBER FROM '{f('dynamic/forum_hasMember_person_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASMODERATOR FROM '{f('dynamic/forum_hasModerator_person_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASINTEREST FROM '{f('dynamic/person_hasInterest_tag_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISLOCATEDIN FROM '{f('dynamic/comment_isLocatedIn_place_0_0.csv')}' (FROM='COMMENT', TO='PLACE', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISLOCATEDIN FROM '{f('dynamic/person_isLocatedIn_place_0_0.csv')}' (FROM='PERSON', TO='PLACE', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISLOCATEDIN FROM '{f('dynamic/post_isLocatedIn_place_0_0.csv')}' (FROM='POST', TO='PLACE', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISLOCATEDIN FROM '{f('static/organisation_isLocatedIn_place_0_0.csv')}' (FROM='ORGANISATION', TO='PLACE', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY KNOWS FROM '{f('dynamic/person_knows_person_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY LIKES FROM '{f('dynamic/person_likes_comment_0_0.csv')}' (FROM='PERSON', TO='COMMENT', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        f"COPY LIKES FROM '{f('dynamic/person_likes_post_0_0.csv')}' (FROM='PERSON', TO='POST', DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY WORKAT FROM '{f('dynamic/person_workAt_organisation_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISPARTOF FROM '{f('static/place_isPartOf_place_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY HASTYPE FROM '{f('static/tag_hasType_tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY ISSUBCLASSOF FROM '{f('static/tagclass_isSubclassOf_tagclass_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
        # f"COPY STUDYAT FROM '{f('dynamic/person_studyAt_organisation_0_0.csv')}' (DELIMITER='|', HEADER=true, quoting=false, escaping=false)",
    ]
    return ddl, copy


def main() -> None:
    args = parse_args()
    data_root: Path = args.data_root
    db_dir: Path = args.db_dir
    derived_dir = Path(args.derived_dir)
    if not derived_dir.is_absolute():
        derived_dir = data_root / derived_dir

    if not data_root.exists():
        raise FileNotFoundError(f"--data-root does not exist: {data_root}")

    derived_dir.mkdir(parents=True, exist_ok=True)
    db_dir.mkdir(parents=True, exist_ok=True)

    overall_memory = PeakRssTracker(args.memory_sample_interval)
    overall_memory.start()
    total_start = time.time()

    print(f"[loader] Building statements (data_root={data_root}) ...")
    ddl, copy = build_statements(data_root, derived_dir)

    print(f"[loader] Opening database: {db_dir}")
    db = neug.Database(str(db_dir), "w", buffer_strategy="M_LAZY", checkpoint_on_close=False)
    conn = db.connect()

    print(f"[loader] Creating schema ({len(ddl)} DDL statements) ...")
    t0 = time.time()
    for stmt in ddl:
        conn.execute(stmt)
    print(f"[loader] Schema created in {time.time() - t0:.2f}s")

    print(f"[loader] Loading data ({len(copy)} COPY statements) ...")
    load_memory = PeakRssTracker(args.memory_sample_interval)
    load_memory.start()
    for i, stmt in enumerate(copy, 1):
        label = stmt.split("FROM")[0].replace("COPY ", "").strip()
        t0 = time.time()
        conn.execute(stmt)
        print(f"  [{i}/{len(copy)}] {label} done in {time.time() - t0:.2f}s")
    load_memory.stop()

    conn.close()
    db.close()
    overall_memory.stop()
    print(
        "[loader] Peak RSS during COPY load: "
        f"{format_bytes(load_memory.peak_bytes())}"
    )
    print(
        "[loader] Peak RSS overall sampled: "
        f"{format_bytes(overall_memory.peak_bytes())}"
    )
    print(
        f"[loader] Peak RSS overall ru_maxrss: {format_bytes(process_peak_rss_bytes())}"
    )
    print(f"[loader] Finished in {time.time() - total_start:.2f}s")


if __name__ == "__main__":
    main()
