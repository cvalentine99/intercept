# INTERCEPT SDR Platform: Deployment Readiness Review

**Date:** 2026-02-17
**Version Reviewed:** 2.19.0
**Verdict:** NOT READY for unattended 30-day deployment without remediation

---

## 1. System Architecture Assessment

### 1.1 SDR Data Flow: RF to Output

INTERCEPT uses a multi-process pipeline architecture. Every signal mode follows the same general pattern:

```
RF Antenna
    |
USB Frontend (RTL-SDR / HackRF / LimeSDR / Airspy / SDRPlay)
    |
[Kernel USB driver → librtlsdr / SoapySDR]
    |
External Decoder Process (subprocess.Popen)
    e.g. rtl_fm, rtl_433, dump1090, acarsdec, AIS-catcher, SatDump
    |
stdout PIPE / PTY / TCP Socket
    |
Python Reader Thread (blocking readline or select/recv)
    |
queue.Queue (maxsize=1000)
    |
SSE Generator (Flask Response, text/event-stream)
    |
Browser / Client
```

**Variant paths:**

| Mode | Frontend Tool | Transport | Parser |
|------|--------------|-----------|--------|
| Pager | `rtl_fm` piped to `multimon-ng` | PTY (via `pty.openpty()`) | Line-based POCSAG/FLEX |
| 433MHz Sensor | `rtl_433 -F json` | stdout PIPE | JSON per line |
| ADS-B | `dump1090 --net` | TCP socket (port 30003, SBS format) | CSV fields |
| ACARS | `acarsdec` | stdout PIPE (PTY on macOS) | JSON per line |
| AIS | `AIS-catcher` | TCP socket (port 10110) | JSON per line |
| Weather Sat | `satdump live` | PTY + file watcher (rglob) | Image files on disk |
| SSTV | `rtl_fm` raw I/Q | stdout PIPE (raw s16 PCM) | Pure-Python DSP |
| APRS | `rtl_fm` piped to `direwolf` | stdout PIPE | APRS packet text |
| RTLAMR | `rtl_tcp` server + `rtlamr` client | Internal TCP | JSON per line |
| SubGHz | `hackrf_transfer` / `hackrf_sweep` | stdout PIPE | Binary/JSON |

### 1.2 Control Plane vs Data Plane Separation

**There is no separation.** Both control (start/stop/configure) and data (SSE streaming) share the same Flask process, the same thread pool, and the same global state in `app.py`.

The control plane is a set of REST endpoints (`POST /pager/start`, `POST /killall`, etc.) that manipulate global `subprocess.Popen` references and `threading.Lock` objects. The data plane is SSE generator functions that pull from `queue.Queue` objects.

**Consequence:** A stuck SSE client consuming a Flask worker thread reduces capacity for control operations. A control operation that acquires a lock blocks data streaming. There is no priority separation.

### 1.3 Timing, Buffering, and Backpressure

**Where buffering exists:**

| Stage | Buffer Type | Size | Backpressure |
|-------|-----------|------|-------------|
| USB → kernel | Kernel ring buffer | 512KB default | Overruns silently dropped |
| Decoder stdout | OS pipe buffer | 64KB (Linux default) | Process blocks on write |
| PTY buffer | Kernel PTY | 4KB typical | Process blocks on write |
| TCP socket (ADS-B/AIS) | Socket recv buffer | 4096 bytes read at a time | TCP flow control |
| Python reader → queue | `queue.Queue` | maxsize=1000 | **See below** |
| Queue → SSE client | HTTP chunked transfer | Unbounded | None |

**Critical backpressure gap:** In `routes/sensor.py:46`, `routes/wifi.py:369`, and most other routes, `queue.put()` is called **without a timeout** on the data path. If the queue is full (1000 items), the reader thread blocks indefinitely. This stalls the reader thread, which stalls the pipe/PTY read, which eventually blocks the external decoder process when the OS pipe buffer fills. The decoder may then drop samples, or worse, the USB driver drops I/Q buffers upstream.

There are two exceptions: `routes/sstv.py:37` and `routes/weather_sat.py:31` both use `queue.put(block=False)` or catch `queue.Full`, discarding events when the queue is full. This is the correct pattern for SDR workloads.

### 1.4 Where the App Is Fragile Under Load or RF Stress

1. **High message rate floods the queue.** A strong 433MHz environment can produce hundreds of sensor events per second from `rtl_433`. Each event goes into a size-1000 queue. With a 1-second SSE timeout, a single client can only drain ~1000 events/sec. Two clients sharing the same queue halve that. In a noisy RF environment, queue saturation is expected.

2. **ADS-B socket buffer accumulation.** In `routes/adsb.py`, SBS data is accumulated in a Python string buffer split by newline. There is no maximum buffer size. If dump1090 sends a burst of data without newlines (malformed or adversarial), the buffer grows without bound.

3. **PTY reader blocking on slow decode.** In pager mode, `select.select()` on the PTY master has a 1-second timeout, which is good. But the audio relay thread (`routes/pager.py:111-165`) reads 4KB chunks from rtl_fm stdout in a tight loop with no rate limiting. If multimon-ng stalls, the relay blocks on `stdin.write()`, stalling rtl_fm, which eventually causes USB overruns.

4. **No process restart on silent failure.** If `rtl_433` silently stops producing output (device reset, USB disconnect, PLL unlock), the reader thread sits in `readline()` forever. There is no watchdog timer, no heartbeat expectation, no "last data received" timestamp to trigger recovery.

### 1.5 Implicit Assumptions That Break in Production

| Assumption | Where | What Breaks |
|-----------|-------|-------------|
| One user at a time per SSE stream | All SSE generators | Multiple clients steal messages from shared queue |
| SDR device always available after claim | `claim_sdr_device()` | USB reset/disconnect between claim and process spawn |
| `pkill -f` only kills our processes | `app.py:730-736` | Kills any system process matching pattern name |
| `/tmp` is private | `constants.py:233-239` | Symlink attacks on multi-user systems |
| Subprocesses exit cleanly on SIGTERM | `safe_terminate()` | Some tools (rtl_fm, hackrf_transfer) ignore SIGTERM under certain states |
| `rtl_test` accurately reports device state | `probe_rtlsdr_device()` | Device can pass probe but fail when actual streaming starts |
| Flask secret key is unimportant | `app.py:54` | Hardcoded key enables session forgery |
| Default credentials are temporary | `config.py:298-299` | `admin:admin` ships as the production default |

---

## 2. SDR Hardware Abstraction & Robustness

### 2.1 Architecture

The SDR abstraction lives in `utils/sdr/` with a clean factory pattern:

```
SDRFactory
  ├── SDRType enum (RTL_SDR, LIME_SDR, HACKRF, AIRSPY, SDRPLAY)
  ├── SDRDevice dataclass (type, index, serial, name, capabilities)
  ├── SDRCapabilities dataclass (freq range, gain range, sample rates, features)
  └── CommandBuilder subclasses per type
        ├── RTLSDRCommandBuilder  → native rtl_* tools
        ├── LimeSDRCommandBuilder → SoapySDR rx_fm/readsb/rx_sdr
        ├── HackRFCommandBuilder  → SoapySDR + hackrf_* tools
        ├── AirspyCommandBuilder  → SoapySDR rx_fm/readsb
        └── SDRPlayCommandBuilder → SoapySDR rx_fm/readsb
```

### 2.2 Device Discovery and Selection

**Detection strategy** (`utils/sdr/detection.py`):
1. RTL-SDR: Native `rtl_test -t` with output parsing
2. HackRF: Native `hackrf_info` with 3-second result caching
3. All others: `SoapySDRUtil --find` with deduplication (skip RTL/HackRF already found natively)

**Problems:**

- **[CRITICAL] Detection is one-shot.** There is no background device enumeration, no USB hotplug listener, no periodic re-scan. If a device is plugged in after startup, the user must manually refresh. If a device is removed during operation, nothing detects this until the subprocess crashes.

- **[HIGH] HackRF cache is not thread-safe.** The `_hackrf_cache` dict in `detection.py` is a module-level global with no lock. Concurrent web requests calling `detect_devices()` can race on cache reads/writes.

- **[HIGH] Only RTL-SDR has pre-flight validation.** `probe_rtlsdr_device(device_index)` runs `rtl_test -d <idx> -t` to verify the device is accessible before spawning a decoder. LimeSDR, Airspy, SDRPlay, and HackRF have **no equivalent probe**. A missing or busy SoapySDR device is only discovered when `rx_fm` or `readsb` fails to start.

- **[MEDIUM] Detection timeout blocks web requests.** `rtl_test -t` has a 5-second timeout. `hackrf_info` has a 10-second timeout. `SoapySDRUtil --find` has a 10-second timeout. These run sequentially. Worst case: a `/devices` request blocks for 25 seconds.

### 2.3 Device Loss and Reset

**Current handling: minimal.**

- When a subprocess dies (device yanked), the reader thread eventually gets EOF on the pipe and exits its loop. The `finally` block calls `safe_terminate()` and `release_sdr_device()`.
- There is **no automatic restart**, no exponential backoff, no notification to the user beyond the SSE stream ending.
- The SDR device registry (`app.py:249`) tracks which mode owns which device index. But if the route handler crashes between `claim_sdr_device()` and the `finally` block, the device stays locked. Recovery requires `POST /killall`.

**Recommendation: Device state machine.**

```
DISCONNECTED → DETECTED → CLAIMED → STREAMING → ERROR → CLAIMED (retry) or RELEASED
```

Each transition should be logged. ERROR → retry should use exponential backoff (1s, 2s, 4s, cap at 30s). After N failures, release the device and notify the user.

### 2.4 Sample Rate Mismatch

`utils/sdr/validation.py` has a `validate_sample_rate()` function with snap-to-nearest logic. This is well-implemented: it checks the device's `SDRCapabilities.sample_rates` list and returns the closest valid rate if `snap_to_nearest=True`.

**Gap:** The snap-to-nearest behavior is silent. If a user requests 22050 Hz (common for pager decoding) but the HackRF's minimum is 2 MHz, the validator snaps to 2 MHz without warning. This produces correct audio **only if** downstream processing accounts for the rate change. In practice, the pager decoder (`multimon-ng`) expects a specific sample rate from `rtl_fm`. The CommandBuilder hardcodes `-s 22050` for RTL-SDR but uses device-appropriate rates for SoapySDR devices. This works, but the rate mismatch path is untested for all device types.

### 2.5 Gain Misconfiguration

Each CommandBuilder maps a single "gain" parameter to device-specific gain stages:

| Device | Gain Mapping | Risk |
|--------|-------------|------|
| RTL-SDR | Single `-g` value (0-49.6 dB) | Low; hardware clamps |
| HackRF | Split: LNA (0-40) + VGA (remaining) | Medium; total can exceed valid range |
| Airspy | Split: LNA (0-15) + MIX (0-15) + VGA (0-15) | Medium; three-stage with hard caps |
| LimeSDR | Single LNAH value (0-73 dB) | Low; hardware clamps |
| SDRPlay | Single IFGR value (0-59 dB) | Low; hardware clamps |

**Issue:** HackRF `_split_gain(62)` produces LNA=40, VGA=22. But `_split_gain(63)` produces LNA=40, VGA=23. The maximum valid VGA is 62 dB, and the UI validation allows up to 62 dB total, so the VGA value will always be valid. However, the gain splitting logic doesn't validate that the resulting LNA+VGA sum produces the intended RF gain. At high gain values, the two-stage split may produce more noise than a single-stage equivalent.

### 2.6 Clock Drift and Unlocked PLLs

**Not addressed in the codebase at all.** RTL-SDR dongles use a 28.8 MHz crystal oscillator with typical drift of 30-100 ppm. The codebase has a `validate_ppm()` function and the `rtl_fm -p <ppm>` flag is used, but:

- There is no automatic PPM calibration
- There is no frequency offset monitoring
- There is no warning when a device's PPM value hasn't been configured (default is 0)
- Non-RTL-SDR devices (LimeSDR, HackRF, Airspy) use TCXO oscillators and PPM is silently ignored, which is correct but not documented to the user

For narrow-band protocols (pagers at 1200 baud, AIS at 9600 baud), uncorrected PPM drift degrades decode rates significantly.

### 2.7 Unsupported Devices and Missing Drivers

Detection gracefully handles missing tools:
```python
if not _check_tool('rtl_test'):
    return []  # No RTL-SDR detection possible
```

The `/dependencies` endpoint checks for installed tools. This is informational only; it does not prevent starting a mode when the required tool is missing. The mode will start, `subprocess.Popen()` will raise `FileNotFoundError`, and the route handler catches this and returns an error response. This is acceptable but could be improved with a pre-flight check.

### 2.8 Recommendations

1. **Add SoapySDR device probe function.** Mirror `probe_rtlsdr_device()` for SoapySDR-based devices using `SoapySDRUtil --probe="driver=lime"` or equivalent. Run before spawning decoder processes.

2. **Add device health watchdog.** Spawn a background thread per active mode that checks `process.poll()` every 5 seconds. On unexpected exit, log the event, release the device, and (optionally) attempt restart with backoff.

3. **Make HackRF cache thread-safe.** Add a `threading.Lock()` around `_hackrf_cache` reads and writes in `detection.py`.

4. **Add PPM calibration guidance.** On first use of an RTL-SDR device, warn the user if PPM is set to 0. Optionally, provide a frequency offset measurement tool (tune to a known reference and measure drift).

5. **Async device detection.** Move `detect_devices()` to a background thread with cached results. Return cached results immediately on web requests. Refresh cache every 30 seconds or on USB events.

---

## 3. Runtime Failure Modes

### CRITICAL

**C1: Blocking `queue.put()` in capture paths**
- **Where:** `routes/sensor.py:46`, `routes/wifi.py:369`, `routes/acars.py:130`, `routes/adsb.py` (via datastore updates), most other routes
- **Mechanism:** Reader thread calls `queue.put(data)` without timeout. If queue is full (1000 items) because no SSE client is consuming, the thread blocks indefinitely.
- **Impact:** Reader thread stall → OS pipe buffer fills → decoder process blocks on stdout write → USB buffer fills → I/Q samples dropped → decoder loses sync → silent data loss or hung process.
- **RF context:** In a high-traffic 433 MHz environment, 100+ events/second is common. Queue fills in 10 seconds.
- **Fix:** Use `queue.put(data, block=False)` and catch `queue.Full`, dropping the oldest event. The SSTV and weather_sat routes already do this correctly.

**C2: Unbounded socket buffer accumulation (ADS-B, AIS)**
- **Where:** `routes/adsb.py` SBS parser, `routes/ais.py` JSON parser
- **Mechanism:** TCP `recv()` data is appended to a Python string buffer, split by newline. If the data source sends data without newlines, the buffer grows without limit.
- **Impact:** Memory exhaustion (OOM kill) under adversarial or malformed input. With ADS-B, a compromised or buggy dump1090 instance could send continuous data without line breaks.
- **RF context:** ADS-B receives data from aircraft transponders. A busy airport can produce thousands of SBS messages per minute. The buffer itself is not the issue for well-formed data, but the lack of a max-size check is.
- **Fix:** Add `MAX_SOCKET_BUFFER = 1024 * 1024` (1 MB). If buffer exceeds this, log a warning and reset.

**C3: No process death detection or restart**
- **Where:** All modes
- **Mechanism:** If a decoder subprocess dies silently (USB disconnect, segfault, OOM), the reader thread exits on EOF. The SSE stream ends. The user sees no data. There is no automatic recovery.
- **Impact:** In a 30-day unattended deployment, USB resets and device disconnects are **certain** to occur. Each one permanently stops the affected mode until a human intervenes.
- **RF context:** RTL-SDR devices are notorious for USB disconnects under thermal stress. HackRF resets on sustained high-bandwidth capture. This is the single most deployment-blocking issue.
- **Fix:** Implement a process watchdog. After subprocess exit, attempt restart with exponential backoff (1s, 2s, 4s, 8s, cap at 60s). After 5 consecutive failures, stop and alert.

**C4: SDR device registry leak on route handler crash**
- **Where:** `app.py:253-282`
- **Mechanism:** `claim_sdr_device()` adds to registry. If the route handler crashes (unhandled exception) before reaching the `finally` block that calls `release_sdr_device()`, the device stays permanently claimed.
- **Impact:** Device locked. Only `POST /killall` clears the registry. In headless operation, this is unrecoverable without manual intervention.
- **Fix:** Use a context manager:
  ```python
  @contextmanager
  def sdr_device_context(device_index, mode_name):
      error = claim_sdr_device(device_index, mode_name)
      if error:
          raise SDRDeviceBusyError(error)
      try:
          yield
      finally:
          release_sdr_device(device_index)
  ```

### HIGH

**H1: SSE fan-out failure — multiple clients steal messages**
- **Where:** All SSE stream endpoints
- **Mechanism:** All SSE clients for a mode share a single `queue.Queue`. `queue.get()` is destructive — once a message is consumed, it's gone. With two clients, each gets roughly half the messages.
- **Impact:** Data loss per client. Not acceptable for monitoring or recording use cases.
- **Fix:** Implement per-client queues with a pub/sub fan-out pattern. Each SSE generator creates its own queue; the reader thread publishes to all active queues.

**H2: Lock ordering deadlock risk**
- **Where:** `app.py:738-809` (`kill_all()` acquires 12+ locks sequentially)
- **Mechanism:** `kill_all()` acquires `process_lock`, then `sensor_lock`, then `wifi_lock`, etc. If a route handler acquires `wifi_lock` then `process_lock` (hypothetical but not prevented), deadlock occurs.
- **Impact:** Application hangs. All modes stop responding.
- **Fix:** Document and enforce a global lock ordering. Alternative: use a single global `modes_lock` for the killall operation with a try-lock timeout.

**H3: Process leaks from incomplete `pkill` coverage**
- **Where:** `utils/process.py:112-121`, `app.py:721-728`
- **Mechanism:** `cleanup_stale_processes()` only kills `rtl_adsb`, `rtl_433`, `multimon-ng`, `rtl_fm`. The `/killall` route kills 20 process names. But neither tracks PIDs. Both use `pkill -f` which matches ALL system processes with that name, not just INTERCEPT's children.
- **Impact:** (1) Orphaned processes from modes not in the kill list (2) Killing unrelated user processes that happen to share a name.
- **Fix:** Track PIDs for all spawned processes. Use `os.kill(pid, signal)` instead of `pkill`. The dump1090 PID file pattern (`utils/process.py:123-207`) is a good model to extend to all modes.

**H4: PTY file descriptor leaks**
- **Where:** `routes/pager.py:381`, `routes/acars.py:317`, `utils/weather_sat.py:446`
- **Mechanism:** `pty.openpty()` returns two file descriptors. The slave is closed immediately; the master is stored for reading. If an exception occurs between `openpty()` and assignment to the instance variable, the master FD leaks.
- **Impact:** After ~500 start/stop cycles (depending on `ulimit -n`), FD exhaustion crashes the application.
- **Fix:** Wrap PTY allocation in try/except and close both FDs on failure.

### MEDIUM

**M1: Blocking `readline()` without timeout**
- **Where:** `routes/sensor.py:37`, `routes/acars.py:110`, `routes/rtlamr.py:39`
- **Mechanism:** `iter(process.stdout.readline, b'')` blocks until EOF or data. If the process hangs (e.g., device in error state producing no data), the thread blocks indefinitely.
- **Impact:** Thread resource leak. Flask's thread pool is finite.
- **Fix:** Use `select.select()` with timeout on the pipe FD, or restructure to use a non-blocking read with a poll loop.

**M2: Memory growth from unmanaged collections**
- **Where:** `app.py:211` (`wifi_handshakes = []`), `app.py:232` (`satellite_passes = []`), `app.py:217` (`bt_services = {}`)
- **Mechanism:** These collections grow without any TTL or size limit. `DataStore` objects are cleaned automatically; these are not.
- **Impact:** Gradual memory increase over days/weeks of operation.
- **Fix:** Apply size limits or TTL cleanup. E.g., cap `wifi_handshakes` at 10000 entries.

**M3: Health check race condition**
- **Where:** `app.py:683-684`
- **Mechanism:** `current_process is not None and (current_process.poll() is None if current_process else False)` — the `current_process` reference can change to `None` between the first check and the `poll()` call.
- **Impact:** Occasional `AttributeError` on `/health` endpoint.
- **Fix:** Capture the reference locally: `proc = current_process; if proc is not None and proc.poll() is None: ...`

### LOW

**L1: Temp files in world-writable `/tmp`**
- **Where:** `constants.py:233-239` (`/tmp/intercept_wifi`, `/tmp/intercept_handshake_`, `/tmp/intercept_direwolf.conf`)
- **Impact:** Symlink attacks on multi-user systems. Not relevant in single-user or container deployments.
- **Fix:** Use `tempfile.mkdtemp()` or a subdirectory under the application's data path.

**L2: Missing tool detection at mode startup**
- **Where:** All routes
- **Mechanism:** If `rtl_fm` is not in PATH, `subprocess.Popen()` raises `FileNotFoundError`. This is caught and returned as an error, but only after the SDR device has been claimed.
- **Impact:** Device claimed, then immediately released. Minor race window.
- **Fix:** Check tool availability before claiming device.

---

## 4. Observability & Telemetry

### 4.1 Current State

The application has a `/health` endpoint that reports:
- Process alive/dead status for each mode
- Data store counts (aircraft, vessels, WiFi networks, BT devices)
- Uptime in seconds
- Version string

This is the **only** observability surface. There is no metrics export, no structured logging, no request tracing.

### 4.2 What Must Exist Before Deployment

**SDR Health Metrics (per device):**

| Metric | Source | Why |
|--------|--------|-----|
| USB read rate (bytes/sec) | Not available in current architecture | Detect USB bandwidth degradation |
| Sample drops / overruns | rtl_fm stderr, dump1090 stderr | Primary indicator of data loss |
| Device temperature (if supported) | RTL-SDR v3 only | Predict thermal shutdown |
| PLL lock status | Not exposed by most tools | Detect frequency drift |
| Time since last valid decode | Application-level timestamp | Detect silent failures |

**RF Quality Metrics:**

| Metric | Source | Why |
|--------|--------|-----|
| RSSI per message | `rtl_433 -M level` (already used) | Characterize RF environment |
| Noise floor estimate | Not currently captured | Distinguish signal from noise |
| Decode success rate | Count valid vs invalid messages | Measure effective sensitivity |
| ADS-B message rate | Count SBS messages per minute | Detect antenna/LNA issues |

**Pipeline Health:**

| Metric | Source | Why |
|--------|--------|-----|
| Queue depth per mode | `queue.Queue.qsize()` | Detect backpressure |
| Queue drops per mode | Counter at `queue.Full` catch | Measure data loss rate |
| SSE client count per mode | Track active generators | Capacity planning |
| Process restart count per mode | Watchdog counter | Detect unstable hardware |
| Subprocess CPU/memory | `/proc/<pid>/stat` | Detect resource runaway |

**Capture Metrics:**

| Metric | Source | Why |
|--------|--------|-----|
| Messages decoded per minute per mode | Application counter | Core throughput metric |
| Unique aircraft/vessels/devices seen | DataStore length | Coverage indicator |
| Decoder process uptime | Track spawn time | Stability indicator |

### 4.3 Recommendations

**Logging structure:**

Current logging uses Python's `logging` module but `configure_logging()` (defined in `config.py:302-310`) is **never called** in `app.py`. All loggers default to WARNING level with no formatting.

Fix: Call `configure_logging()` at startup. Switch to structured JSON logging for machine parsing:

```python
import json
import logging

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            'timestamp': self.formatTime(record),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'line': record.lineno,
        })
```

**Metrics export:**

Add a `/metrics` endpoint exporting Prometheus-compatible text format. Minimal implementation:

```python
@app.route('/metrics')
def prometheus_metrics():
    lines = []
    lines.append(f'intercept_uptime_seconds {time.time() - _app_start_time}')
    for mode, q in mode_queues.items():
        lines.append(f'intercept_queue_depth{{mode="{mode}"}} {q.qsize()}')
    for mode, counter in decode_counters.items():
        lines.append(f'intercept_messages_total{{mode="{mode}"}} {counter}')
    for store_name, store in data_stores.items():
        lines.append(f'intercept_datastore_size{{store="{store_name}"}} {len(store)}')
    return Response('\n'.join(lines), mimetype='text/plain')
```

**On-device vs remote visibility:**

For headless deployment:
- Log to stdout (captured by Docker/systemd journal)
- Expose `/health` and `/metrics` on a management port or authenticated endpoint
- Configure `INTERCEPT_ALERT_WEBHOOK_URL` for critical alerts (device failure, process crash)
- Consider a lightweight dashboard (Grafana with Prometheus scraping `/metrics`)

---

## 5. Configuration & Deployment Model

### 5.1 Hardcoded Parameters

| Parameter | Location | Value | Risk |
|-----------|----------|-------|------|
| Flask secret key | `app.py:54` | `"signals_intelligence_secret"` | **CRITICAL:** Enables session forgery |
| Admin password | `config.py:299` | `"admin"` | **CRITICAL:** Default credential |
| Admin username | `config.py:298` | `"admin"` | **HIGH:** Guessable |
| Postgres password | `docker-compose.yml:113` | `"intercept"` | **HIGH:** Default credential |
| ADS-B DB password | `config.py:254` | `"intercept"` | **HIGH:** Matches Postgres default |
| Webhook secret | `config.py:294` | `""` (empty) | **MEDIUM:** Unsigned webhooks |
| Temp file paths | `constants.py:233-239` | `/tmp/intercept_*` | **LOW:** Predictable |
| Process kill list | `app.py:721-728` | 20 process names | **MEDIUM:** Incomplete, not PID-tracked |

### 5.2 Runtime Configuration Strategy

The configuration model is well-designed. `config.py` uses a consistent pattern:
```python
VALUE = _get_env('KEY', default)
```

All settings use the `INTERCEPT_` prefix. Type-safe parsing is provided for int, float, bool, and string. This is a solid pattern.

**Problems:**
1. **Duplication with `constants.py`.** Both files define timeouts, ports, and intervals. `config.py` values appear unused in routes; routes import from `constants.py` instead. Example: `config.py:233` defines `PROCESS_TIMEOUT=5` but routes use `constants.py:29` `PROCESS_TERMINATE_TIMEOUT=2`.

2. **No validation.** Port numbers, timeout values, and database URLs are accepted without range checking. `INTERCEPT_PORT=99999` would pass config parsing but fail at bind time with an opaque error.

3. **Secrets in environment variables are acceptable** for container deployments but should be documented as requiring override. The default values should not be functional credentials.

### 5.3 Environment Variable Safety

Environment variables are read at import time (module level). This means:
- Changing an environment variable after startup has no effect
- All configuration is frozen at process start
- This is correct for a container deployment where config changes require a restart

No environment variables are logged, which is good for secret protection. However, the `admin:admin` default means a deployment that forgets to set `INTERCEPT_ADMIN_PASSWORD` is immediately compromised.

### 5.4 Headless Startup Requirements

The application starts with:
```bash
python intercept.py  # or CMD ["python", "intercept.py"] in Docker
```

**Startup sequence issues:**
1. `configure_logging()` is never called. Logging defaults are unformatted.
2. Database initialization (`init_db()`) has no error handling. If SQLite cannot create its file (permissions, disk full), the app starts but crashes on first DB query.
3. TLE update runs in a daemon thread at startup. If network is unavailable, this silently fails (acceptable).
4. Stale process cleanup runs at startup. `cleanup_stale_processes()` kills rtl_adsb, rtl_433, multimon-ng, rtl_fm system-wide with SIGKILL. On a shared system, this destroys other users' SDR processes.

### 5.5 Recommended Production Configuration Model

```bash
# Required — no defaults, app should refuse to start without these:
INTERCEPT_FLASK_SECRET_KEY=<random 32+ char string>
INTERCEPT_ADMIN_PASSWORD=<strong password>

# Recommended overrides:
INTERCEPT_LOG_LEVEL=INFO
INTERCEPT_HOST=0.0.0.0
INTERCEPT_PORT=5050

# If using ADS-B history:
INTERCEPT_ADSB_DB_PASSWORD=<strong password>

# SDR defaults (adjust per hardware):
INTERCEPT_DEFAULT_GAIN=40
INTERCEPT_DEFAULT_DEVICE=0
```

### 5.6 Example Deployment Layout

```
/opt/intercept/
├── docker-compose.yml          # Deployment config
├── .env                        # Secrets (chmod 600, not in git)
│   ├── INTERCEPT_FLASK_SECRET_KEY=...
│   ├── INTERCEPT_ADMIN_PASSWORD=...
│   └── INTERCEPT_ADSB_DB_PASSWORD=...
├── data/                       # Persisted data (bind mount)
│   ├── weather_sat/            # Decoded satellite images
│   ├── sstv/                   # SSTV images
│   └── intercept.db            # SQLite database
└── pgdata/                     # Postgres data (if history profile)
```

---

## 6. Security & Abuse Considerations

### 6.1 Unsafe Shell Usage

**No `shell=True` usage found anywhere in the codebase.** All subprocess calls use list-based command construction. This is a strong security posture.

### 6.2 External Tool Invocation

All external tools are invoked via `subprocess.Popen()` or `subprocess.run()` with argument lists. Input parameters (frequencies, gains, device indices) are validated by `utils/validation.py` before being passed to command builders.

**Remaining risks:**
- `pkill -f <pattern>` (`app.py:732`) matches against all system processes. An attacker who can trigger `/killall` can kill arbitrary processes by name.
- Network interface names passed to `airodump-ng` could theoretically contain shell metacharacters, but since `shell=True` is not used, this is not exploitable via command injection. The `validate_network_interface()` function enforces alphanumeric + dash, max 15 chars, which is sufficient.

### 6.3 Untrusted Input Paths

| Input | Validation | Risk |
|-------|-----------|------|
| Frequency (MHz) | `validate_frequency()`: 24-1766 MHz | Low |
| Gain (dB) | `validate_gain()`: 0-50 | Low |
| Device index | `validate_device_index()`: 0-255 | Low |
| Network interface | `validate_network_interface()`: regex | Low |
| MAC address | `validate_mac_address()`: regex | Low |
| File paths (weather_sat) | `is_relative_to(allowed_base)` | **Protected** |
| File paths (SSTV decode_file) | Existence check only | **VULNERABLE** — no path traversal check |
| Admin credentials | Plain comparison | **No brute-force protection** beyond rate limiting on login |
| SSE event data | HTML escaped for display fields | Low |
| SBS/AIS socket data | Parsed as text, no exec | Low |

**SSTV path traversal:** `utils/sstv/sstv_decoder.py` `decode_file()` accepts an arbitrary path and reads it as audio. There is no validation that the path is within the application's data directory. An attacker with access to the web API could read the first N bytes of any file readable by the process (reinterpreted as audio, but still a file read primitive).

### 6.4 Privilege Requirements

| Capability | Why | Can Avoid Root? |
|------------|-----|----------------|
| USB device access | RTL-SDR, HackRF, etc. | Yes: udev rules for `plugdev` group |
| WiFi monitor mode | `airmon-ng`, raw 802.11 capture | No: requires `CAP_NET_RAW` + `CAP_NET_ADMIN` |
| Bluetooth scanning | Some HCI operations | Partial: BlueZ DBus API avoids root for BLE |
| Packet capture | tshark/tcpdump (if used) | Yes: `CAP_NET_RAW` only |

**Docker container runs as `privileged: true`**, which grants ALL Linux capabilities plus full device access. This is the maximum possible attack surface.

**Mitigation for Docker:**
```yaml
cap_add:
  - NET_RAW
  - NET_ADMIN
  - SYS_RAWIO      # USB device access
cap_drop:
  - ALL
security_opt:
  - no-new-privileges:true
```

**Note:** `SYS_RAWIO` with explicit device mapping may not be sufficient for all USB SDR operations. Testing is required. Full `privileged: true` may remain necessary for some SDR hardware.

### 6.5 Network Exposure

- Port 5050 is exposed on `0.0.0.0` (all interfaces) by default
- No TLS/HTTPS configuration
- No CSRF protection on state-changing POST endpoints
- Rate limiting exists but only on the login endpoint (`5/minute`)
- The `/killall` endpoint has no rate limiting — an attacker can repeatedly kill all processes
- SSE streams have no authentication check (session-based auth may apply if login is enforced)

**For an SDR interception platform, network exposure should be minimized.** Bind to `127.0.0.1` by default and require explicit opt-in for network access. Use a reverse proxy (nginx, caddy) for TLS termination and authentication.

### 6.6 Adversarial Curiosity: SDR-Specific Risks

This platform receives and processes RF signals from untrusted sources. Adversarial considerations:

1. **Crafted ADS-B messages.** ADS-B has no authentication. An attacker with a transmitter can inject fake aircraft data. The platform will display and store it as real. This is an inherent limitation of ADS-B, not a software bug, but it should be documented.

2. **433 MHz replay attacks.** The SubGHz mode (`routes/subghz.py`) supports HackRF transmission. An attacker with web access could transmit on arbitrary frequencies within ISM bands. TX is restricted to ISM bands (`constants.py:264-268`) but within those bands, arbitrary signals can be sent.

3. **Denial of service via RF.** Jamming the SDR's receive frequency causes decode failures. The platform has no jamming detection.

4. **WebSocket audio streaming.** If enabled, raw audio from the SDR is streamed via WebSocket. An attacker with access could listen to live RF demodulation, which may include voice communications.

---

## 7. Production Hardening Checklist

**"What must be done before this can run unattended for 30 days?"**

### Code Changes (Required)

- [ ] **Fix blocking `queue.put()` in all capture paths.** Use `block=False` with `queue.Full` handling. Drop oldest event on overflow. (Affects: `sensor.py`, `wifi.py`, `acars.py`, `pager.py`, `adsb.py`, `ais.py`, `aprs.py`, `rtlamr.py`, `dsc.py`)
- [ ] **Add max buffer size to ADS-B and AIS socket parsers.** Cap at 1 MB; reset buffer and log warning on overflow.
- [ ] **Implement process watchdog.** Background thread per mode checks `process.poll()` every 5 seconds. On unexpected exit: log, release device, optionally restart with exponential backoff.
- [ ] **Replace shared SSE queue with per-client fan-out.** Each SSE generator gets its own bounded queue. Reader thread publishes to all active queues.
- [ ] **Add SDR device context manager.** Guarantee device release even on unhandled exceptions.
- [ ] **Call `configure_logging()` at startup.** In `app.py` `main()`, before any other initialization.
- [ ] **Require non-default Flask secret key.** Refuse to start if `INTERCEPT_FLASK_SECRET_KEY` is not set.
- [ ] **Require non-default admin password.** Refuse to start (or print prominent warning) if `INTERCEPT_ADMIN_PASSWORD` is `admin`.
- [ ] **Add path traversal check to SSTV `decode_file()`.** Mirror the pattern in `utils/weather_sat.py:264-280`.
- [ ] **Fix health check race condition.** Capture process references locally before calling `poll()`.
- [ ] **Add `/metrics` endpoint.** Export queue depths, message counts, process status, uptime in Prometheus text format.

### Code Changes (Recommended)

- [ ] Consolidate `config.py` and `constants.py`. Remove duplicated timeout/interval values.
- [ ] Add PID tracking for all spawned processes (extend dump1090 PID file pattern).
- [ ] Add `queue.Full` exception handling to remaining `queue.put()` calls that use timeout.
- [ ] Add PTY FD leak protection (try/except around `pty.openpty()`).
- [ ] Make HackRF detection cache thread-safe.
- [ ] Add SoapySDR device probe function for non-RTL-SDR hardware.
- [ ] Add database initialization error handling (catch and log, not crash later).
- [ ] Cap unbounded collections (`wifi_handshakes`, `satellite_passes`, `bt_services`).
- [ ] Add CSRF protection to state-changing POST endpoints.

### System-Level Requirements

- [ ] **File descriptor limit.** Set `ulimit -n 4096` minimum. In Docker, add to compose:
  ```yaml
  ulimits:
    nofile:
      soft: 4096
      hard: 8192
  ```
- [ ] **USB autosuspend disabled.** RTL-SDR devices enter USB autosuspend and become unresponsive:
  ```bash
  echo -1 > /sys/module/usbcore/parameters/autosuspend
  # Or per-device:
  echo on > /sys/bus/usb/devices/<device>/power/control
  ```
- [ ] **Kernel blacklist for DVB-T driver.** RTL-SDR devices are claimed by the DVB-T kernel module by default:
  ```bash
  echo "blacklist dvb_usb_rtl28xxu" > /etc/modprobe.d/blacklist-rtlsdr.conf
  echo "blacklist rtl2832" >> /etc/modprobe.d/blacklist-rtlsdr.conf
  ```
- [ ] **USB power management.** On systems with USB hubs, ensure ports provide adequate current (500mA+ for RTL-SDR, 500mA for HackRF). Use powered USB hubs for multiple SDR devices.
- [ ] **Thermal management.** RTL-SDR V3 devices throttle above 70C. Ensure adequate airflow. Consider heatsinks for 24/7 operation.
- [ ] **Network access for TLE updates.** Satellite tracking requires periodic downloads from celestrak.org. Ensure DNS resolution and HTTPS outbound access on port 443.

### Kernel / USB Considerations

- [ ] **USB reset recovery.** The `usbreset` utility can reset a hung USB device without physical intervention:
  ```bash
  apt install usbutils
  usbreset <vendor_id>:<product_id>
  ```
  Integrate into the process watchdog: on 3 consecutive process failures, attempt USB device reset.
- [ ] **udev rules for non-root USB access:**
  ```
  # /etc/udev/rules.d/20-rtlsdr.rules
  SUBSYSTEM=="usb", ATTRS{idVendor}=="0bda", ATTRS{idProduct}=="2838", GROUP="plugdev", MODE="0666"
  # /etc/udev/rules.d/20-hackrf.rules
  SUBSYSTEM=="usb", ATTRS{idVendor}=="1d50", ATTRS{idProduct}=="6089", GROUP="plugdev", MODE="0666"
  ```
- [ ] **USB buffer size.** For high sample rate captures, increase the USB buffer:
  ```bash
  echo 0 > /sys/module/usbcore/parameters/usbfs_memory_mb
  # Or set to a specific value:
  echo 256 > /sys/module/usbcore/parameters/usbfs_memory_mb
  ```
  Note: Setting to 0 removes the limit entirely (Linux 5.4+).

### Hardware Watchdog Strategies

- [ ] **Software watchdog.** Add to the process supervisor: if no data is received from any active mode for 5 minutes, restart the decoder process. If 3 restarts fail, attempt USB device reset. If USB reset fails, log critical alert.
- [ ] **Hardware watchdog (optional).** For bare-metal deployments, use a hardware watchdog timer (e.g., `/dev/watchdog`). The application periodically writes to the watchdog device. If the application hangs, the watchdog reboots the system.
- [ ] **Container restart policy.** `docker-compose.yml` already has `restart: unless-stopped`. Verify the health check is working and Docker will restart the container on health check failure.

### Restart / Recovery Behavior

- [ ] **Stale process cleanup at startup.** Already implemented but incomplete. Extend `cleanup_stale_processes()` to cover all process names in the kill list.
- [ ] **Database journal recovery.** SQLite in WAL mode recovers automatically from crashes. Verify WAL mode is enabled: `PRAGMA journal_mode=WAL;`
- [ ] **Data directory permissions.** Ensure `/app/data` is writable by the application user. In Docker, verify the bind mount has correct permissions.
- [ ] **Graceful shutdown.** Register a SIGTERM handler that:
  1. Stops accepting new SSE connections
  2. Terminates all decoder processes with SIGTERM
  3. Waits up to 5 seconds for process exit
  4. Force-kills remaining processes
  5. Flushes database writes
  6. Exits

---

## 8. Optional Recommendations

### 8.1 Containerization (with SDR Constraints)

The existing Docker setup is functional but needs hardening:

**Current issues:**
- `privileged: true` grants all capabilities
- `/dev/bus/usb` exposes ALL USB devices
- No resource limits (CPU, memory)
- No read-only filesystem

**Production-hardened docker-compose.yml:**
```yaml
services:
  intercept:
    image: intercept:latest
    build: .
    container_name: intercept
    ports:
      - "127.0.0.1:5050:5050"  # Bind to localhost only
    # Specific capabilities instead of privileged
    cap_add:
      - NET_RAW
      - NET_ADMIN
    # Keep privileged: true ONLY if cap_add is insufficient for USB
    privileged: true
    devices:
      - /dev/bus/usb:/dev/bus/usb
    volumes:
      - ./data:/app/data
    env_file:
      - .env  # Secrets in a separate file
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '2.0'
    ulimits:
      nofile:
        soft: 4096
        hard: 8192
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:5050/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 15s
    logging:
      driver: json-file
      options:
        max-size: "50m"
        max-file: "5"
```

**Reality check on `privileged: true`:** For USB SDR access in Docker, `privileged: true` is often genuinely required. The combination of `SYS_RAWIO` + device mapping works for some devices but not all. Test with your specific hardware. If you can avoid `privileged: true`, do so. If you cannot, document why.

### 8.2 systemd Service Design

For bare-metal deployment without Docker:

```ini
# /etc/systemd/system/intercept.service
[Unit]
Description=INTERCEPT Signal Intelligence Platform
After=network.target bluetooth.target
Wants=bluetooth.target

[Service]
Type=simple
User=intercept
Group=plugdev
WorkingDirectory=/opt/intercept
EnvironmentFile=/opt/intercept/.env
ExecStart=/opt/intercept/venv/bin/python intercept.py
ExecStop=/bin/kill -TERM $MAINPID
Restart=on-failure
RestartSec=10
WatchdogSec=300

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
ReadWritePaths=/opt/intercept/data /opt/intercept/instance
PrivateTmp=yes

# Capabilities for SDR and network
AmbientCapabilities=CAP_NET_RAW CAP_NET_ADMIN
CapabilityBoundingSet=CAP_NET_RAW CAP_NET_ADMIN

# Resource limits
LimitNOFILE=4096
MemoryMax=2G
CPUQuota=200%

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=intercept

[Install]
WantedBy=multi-user.target
```

**Note:** This service definition does NOT grant USB device access without additional udev rules. The `plugdev` group + udev rules (Section 7) handle USB permissions.

### 8.3 Hardware-in-the-Loop Testing Strategy

For validating deployment readiness with actual SDR hardware:

**Test matrix:**

| Test | Hardware | Duration | Pass Criteria |
|------|----------|----------|---------------|
| Basic decode | RTL-SDR + antenna | 1 hour | Messages decoded, no crashes |
| Device yank | RTL-SDR | Manual | Graceful error, device released, no orphan processes |
| Device re-plug | RTL-SDR | Manual | Device re-detected, mode restartable |
| Thermal stress | RTL-SDR in enclosure | 4 hours | No thermal shutdown, decode rate stable |
| Multi-device | 2x RTL-SDR | 1 hour | Both modes run simultaneously, no cross-contamination |
| High traffic | RTL-SDR + RF generator | 1 hour | Queue overflow handled, no OOM, no hung threads |
| Long soak | RTL-SDR | 72 hours | Memory stable, no FD leaks, decode rate stable |
| USB hub stress | RTL-SDR via unpowered hub | 1 hour | Handle USB power-related failures gracefully |
| Concurrent modes | RTL-SDR + HackRF | 1 hour | Both devices claimed correctly, no registry conflicts |

**Automation:**
1. Use `rtl_test -s 2048000 -t` to verify device function before each test
2. Monitor `/proc/<pid>/fd` count for FD leaks
3. Monitor RSS via `/proc/<pid>/status` for memory leaks
4. Log all process restarts and queue drops
5. Compare decode count at test start vs end for drift detection

---

## Summary of Findings by Severity

| Severity | Count | Key Issues |
|----------|-------|------------|
| **CRITICAL** | 4 | Blocking queue.put, unbounded socket buffers, no process restart, device registry leak |
| **HIGH** | 4 | SSE fan-out failure, lock ordering risk, process leak via pkill, PTY FD leaks |
| **MEDIUM** | 3 | Blocking readline, unmanaged memory growth, health check race |
| **LOW** | 2 | Temp file paths, late tool detection |
| **Security-CRITICAL** | 3 | Hardcoded Flask secret, default admin credentials, SSTV path traversal |
| **Security-HIGH** | 2 | Docker privileged mode, no TLS |

**Deployment verdict:** The application architecture is sound and well-structured. The SDR abstraction layer, input validation, and process management patterns demonstrate competent engineering. However, the critical runtime issues (blocking I/O, no process restart, no watchdog) make it unsuitable for unattended operation without remediation. The security issues (default credentials, hardcoded secret) make it unsuitable for any networked deployment without fix.

After addressing the critical and high items from the checklist in Section 7, the platform should be capable of reliable 30-day unattended operation with appropriate hardware and system-level preparation.
