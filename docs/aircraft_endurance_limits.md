# Aircraft endurance limits encoded in feasibility checker

The aircraft endurance check currently uses hard-coded passenger-based profiles for the Praetor 500, Legacy 450 (E545), Citation CJ3+, and Citation CJ2+. Other supported types fall back to simple aircraft-level limits.

## Passenger-based endurance profiles

Each table shows the maximum planned block time allowed before applying the 15-minute caution buffer, using hours:minutes formatting by passenger count.

### Praetor 500

| Pax | Max block time |
| --- | -------------- |
| 0 | 7:15 |
| 1 | 7:15 |
| 2 | 7:15 |
| 3 | 7:15 |
| 4 | 7:15 |
| 5 | 7:00 |
| 6 | 6:45 |
| 7 | 6:35 |
| 8 | 6:25 |
| 9 | 6:15 |

### Legacy 450 (E545)

| Pax | Max block time |
| --- | -------------- |
| 0 | 6:25 |
| 1 | 6:25 |
| 2 | 6:20 |
| 3 | 6:10 |
| 4 | 6:00 |
| 5 | 5:50 |
| 6 | 5:45 |
| 7 | 5:35 |
| 8 | 5:25 |
| 9 | 5:15 |

### Citation CJ3+

| Pax | Max block time |
| --- | -------------- |
| 0 | 4:40 |
| 1 | 4:40 |
| 2 | 4:40 |
| 3 | 4:20 |
| 4 | 4:05 |
| 5 | 3:45 |
| 6 | 3:30 |
| 7 | 3:15 (conditional) |
| 8 | — |
| 9 | — |

### Citation CJ2+

| Pax | Max block time |
| --- | -------------- |
| 0 | 3:45 |
| 1 | 3:45 |
| 2 | 3:45 |
| 3 | 3:25 |
| 4 | 3:10 |
| 5 | 2:55 |
| 6 | 2:35 |
| 7 | 0:00 (conditional) |
| 8 | — |
| 9 | — |

## Aircraft-level default limits

Aircraft without a passenger profile use a single endurance limit. Any block time at or above the limit fails; up to 10% or 20 minutes below (whichever is tighter) yields a caution.

| Aircraft | Endurance limit |
| -------- | --------------- |
| Citation CJ2+ | 3:30 |
| Citation CJ3+ | 3:40 |
| Citation CJ4 | 3:50 |
| Pilatus PC-12 | 4:00 |
| All others (default) | 4:00 |

Refer to `feasibility/checker_aircraft.py` for the authoritative values used by the checker.
