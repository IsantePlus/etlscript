#!/usr/bin/env python3
"""
Automated runner for the patient_status_arv ETL comparison test.

Loads DDLs, test data, wraps each flat SQL file in a stored procedure,
then runs the comparison script that diffs the results.
"""

import argparse
import getpass
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(
        description='Run the patient_status_arv ETL comparison test end-to-end',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    db_group = parser.add_argument_group('Database connection')
    db_group.add_argument('--host', '-H', default='localhost',
                          help='MySQL host (default: localhost)')
    db_group.add_argument('--port', '-P', type=int, default=3306,
                          help='MySQL port (default: 3306)')
    db_group.add_argument('--user', '-u', help='MySQL username')
    db_group.add_argument('--password', '-p', help='MySQL password')

    path_group = parser.add_argument_group('File paths')
    path_group.add_argument('--ddl-dir', type=Path,
                            default=REPO_ROOT / 'reports_ddl',
                            help='Directory containing DDL SQL files')
    path_group.add_argument('--test-data-dir', type=Path,
                            default=REPO_ROOT / 'reports_dml_test_01',
                            help='Directory containing test data SQL files')
    path_group.add_argument('--current-sql', type=Path,
                            default=REPO_ROOT / 'patient_status_arv_dml.sql',
                            help='Current (production) flat SQL file')
    path_group.add_argument('--new-sql', type=Path,
                            default=REPO_ROOT / 'sql_files' / 'patient_status_arv_dml.sql',
                            help='New (modified) flat SQL file')
    path_group.add_argument('--comparison-sql', type=Path,
                            default=REPO_ROOT / 'test' / 'test_patient_status_arv_dml_comparison.sql',
                            help='Comparison test SQL script')

    return parser.parse_args()


def mysql_cmd(args):
    """Build the base mysql CLI command from parsed args."""
    return [
        'mysql', '--protocol=tcp',
        f'-h{args.host}',
        f'-P{args.port}',
        f'-u{args.user}',
        f'-p{args.password}',
    ]


# Matches mysql's "Using a password on the command line interface can be insecure"
_PASSWORD_WARNING_RE = re.compile(
    r'^mysql: \[Warning\].*password.*insecure.*$', re.IGNORECASE
)


def run_mysql(args, *, input_text=None, input_file=None, capture_stdout=False):
    """Execute a mysql command, streaming from input_text or input_file.

    Returns (stdout_text, stderr_text). Raises SystemExit on failure.
    """
    cmd = mysql_cmd(args)
    stdin_fh = None

    if input_text is not None:
        kwargs = dict(input=input_text.encode('utf-8'))
    elif input_file is not None:
        stdin_fh = open(input_file, 'rb')
        kwargs = dict(stdin=stdin_fh)
    else:
        kwargs = {}

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE if capture_stdout else None,
            stderr=subprocess.PIPE,
            **kwargs,
        )
    finally:
        if stdin_fh is not None:
            stdin_fh.close()

    # Filter the standard password warning from stderr
    stderr_lines = result.stderr.decode('utf-8', errors='replace').splitlines()
    filtered_stderr = '\n'.join(
        line for line in stderr_lines if not _PASSWORD_WARNING_RE.match(line)
    ).strip()

    if result.returncode != 0:
        if filtered_stderr:
            print(filtered_stderr, file=sys.stderr)
        sys.exit(1)

    stdout_text = result.stdout.decode('utf-8', errors='replace') if capture_stdout else ''
    return stdout_text, filtered_stderr


def load_sql_dir(args, directory, step_num, total_steps, label):
    """Load all .sql files from a directory in sorted order."""
    sql_files = sorted(directory.glob('*.sql'))
    if not sql_files:
        print(f'[{step_num}/{total_steps}] Warning: no .sql files in {directory}',
              file=sys.stderr)
        return

    for sql_file in sql_files:
        print(f'[{step_num}/{total_steps}] Loading {label}: {sql_file.name} ... ',
              end='', file=sys.stderr, flush=True)
        run_mysql(args, input_file=sql_file)
        print('done', file=sys.stderr)


def wrap_in_procedure(sql_path, proc_name):
    """Read a flat SQL file and wrap its body in a CREATE PROCEDURE statement.

    Strips the leading USE isanteplus; line (not allowed inside a procedure body)
    and re-emits it before the DELIMITER block.
    """
    sql_content = sql_path.read_text(encoding='utf-8')
    sql_content = re.sub(r'(?i)^\s*USE\s+isanteplus\s*;\s*\n?', '', sql_content, count=1)

    return (
        f'USE isanteplus;\n'
        f'DELIMITER $$\n'
        f'DROP PROCEDURE IF EXISTS {proc_name}$$\n'
        f'CREATE PROCEDURE {proc_name}()\n'
        f'BEGIN\n'
        f'{sql_content}\n'
        f'END$$\n'
        f'DELIMITER ;\n'
    )


def create_procedure(args, sql_path, proc_name, step_num, total_steps):
    """Wrap a flat SQL file in a stored procedure and load it."""
    print(f'[{step_num}/{total_steps}] Creating stored procedure: {proc_name} ... ',
          end='', file=sys.stderr, flush=True)
    wrapped_sql = wrap_in_procedure(sql_path, proc_name)
    run_mysql(args, input_text=wrapped_sql)
    print('done', file=sys.stderr)


def preflight(args):
    """Verify prerequisites before running any SQL."""
    if shutil.which('mysql') is None:
        print('Error: mysql client not found on PATH', file=sys.stderr)
        sys.exit(1)

    missing = []
    for path, desc in [
        (args.ddl_dir, '--ddl-dir'),
        (args.test_data_dir, '--test-data-dir'),
        (args.current_sql, '--current-sql'),
        (args.new_sql, '--new-sql'),
        (args.comparison_sql, '--comparison-sql'),
    ]:
        if not path.exists():
            missing.append(f'  {desc}: {path}')
    if missing:
        print('Error: missing files/directories:', file=sys.stderr)
        print('\n'.join(missing), file=sys.stderr)
        sys.exit(1)


def format_table(headers, rows):
    """Format tab-separated data as an ASCII box table."""
    all_rows = [headers] + rows
    col_widths = [
        max(len(row[i]) for row in all_rows)
        for i in range(len(headers))
    ]

    def rule():
        return '+' + '+'.join('-' * (w + 2) for w in col_widths) + '+'

    def data_row(row):
        cells = ' | '.join(val.ljust(w) for val, w in zip(row, col_widths))
        return f'| {cells} |'

    lines = [rule(), data_row(headers), rule()]
    for row in rows:
        lines.append(data_row(row))
    lines.append(rule())
    return '\n'.join(lines)


def _split_result_sets(text):
    """Split mysql batch output into individual result sets.

    MySQL batch mode concatenates result sets with no blank-line separator.
    Each result set has a header row followed by zero or more data rows, all
    with the same number of tab-separated columns.  We detect a new result set
    whenever the column count changes.
    """
    result_sets = []
    current = []          # lines in the current result set
    current_ncols = None  # column count of the current result set

    for line in text.splitlines():
        if not line:
            continue
        ncols = line.count('\t') + 1
        if current_ncols is None or ncols != current_ncols:
            # New result set starts here
            if current:
                result_sets.append(current)
            current = [line]
            current_ncols = ncols
        else:
            current.append(line)

    if current:
        result_sets.append(current)

    return result_sets


def format_mysql_output(text):
    """Parse mysql batch output into pretty-printed result sets.

    MySQL batch mode (non-interactive) outputs tab-separated values with
    column headers, one result set per SELECT, concatenated with no separator.
    """
    parts = []

    for lines in _split_result_sets(text):
        headers = lines[0].split('\t')
        rows = [line.split('\t') for line in lines[1:]]

        # Single-column "comparison" labels: print as a section header
        if headers == ['comparison'] and len(rows) == 1:
            parts.append(f'\n--- {rows[0][0]} ---')
        # Multi-column data: pretty-print as a box table
        elif len(headers) > 1 and rows:
            parts.append(format_table(headers, rows))
        # Single-column with no data (empty diff result): skip silently
        elif not rows:
            continue
        # Anything else (e.g. mysql warnings): pass through as-is
        else:
            parts.append('\n'.join(lines))

    return '\n'.join(parts)


def main():
    args = parse_args()

    if args.user is None:
        args.user = input('MySQL username: ')
    if args.password is None:
        args.password = getpass.getpass('MySQL password: ')

    preflight(args)

    total_steps = 5

    # Step 1: Load DDLs
    load_sql_dir(args, args.ddl_dir, 1, total_steps, 'DDL')

    # Step 2: Load test data
    load_sql_dir(args, args.test_data_dir, 2, total_steps, 'test data')

    # Step 3: Wrap current SQL in stored procedure
    create_procedure(args, args.current_sql, '_test_current_version', 3, total_steps)

    # Step 4: Wrap new SQL in stored procedure
    create_procedure(args, args.new_sql, '_test_new_version', 4, total_steps)

    # Step 5: Run comparison
    print(f'[5/{total_steps}] Running comparison script ... ',
          end='', file=sys.stderr, flush=True)
    stdout, _ = run_mysql(args, input_file=args.comparison_sql, capture_stdout=True)
    print('done', file=sys.stderr)

    print('\n=== Comparison Results ===', file=sys.stderr)
    print(format_mysql_output(stdout))


if __name__ == '__main__':
    main()
