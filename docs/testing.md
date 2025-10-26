# Running the Test Suite

The repository ships with an automated test suite so you can quickly verify that
recent code changes (or the generated test cases) behave as expected. The tests
are implemented with [pytest](https://docs.pytest.org/), and live under the
[`tests/`](../tests) directory.

## 1. Set up a virtual environment (recommended)

```bash
python -m venv .venv
source .venv/bin/activate
```

If you already work inside a virtual environment you can keep using that and
skip this step.

## 2. Install the dependencies

Install the Python packages that the project and its tests rely on:

```bash
pip install -r requirements.txt
pip install pytest
```

`requirements.txt` contains the runtime dependencies for the tools themselves,
while `pytest` provides the testing framework.

## 3. Run all tests

From the repository root (the folder that contains this file), execute:

```bash
pytest
```

This command automatically discovers and runs every test module under the
`tests/` directory. A green `PASSED` report means everything is working as
expected. If something fails, pytest prints the failing test name alongside a
traceback showing what went wrong.

## 4. Run an individual test module or test case

To focus on a specific module, supply its path:

```bash
pytest tests/test_flight_following_reports.py
```

You can also drill down to an individual test case by adding `-k` with part of
its name:

```bash
pytest tests/test_flight_following_reports.py -k "diagnostics"
```

## 5. Re-running the same test quickly

Pytest caches the list of collected tests, so re-running a single file after you
make code changes is fast:

```bash
pytest tests/test_flight_following_reports.py --lf
```

The `--lf` (last-failed) flag only executes the tests that failed in the
previous run, which is helpful when iterating on a fix.

## 6. Keeping the test suite healthy

Whenever you introduce new functionality, add or update tests in the `tests/`
folder to cover the change, then run `pytest` again. This workflow ensures the
helpers we build together keep working for you over time.
