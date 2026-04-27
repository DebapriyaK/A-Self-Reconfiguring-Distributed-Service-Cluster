import subprocess
import threading


def stream_output(process, lines):
    for line in iter(process.stdout.readline, ""):
        lines.append(line)
    process.stdout.close()


def start_process(cmd, env, cwd):
    return subprocess.Popen(
        cmd, cwd=cwd, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def start_monitored(cmd, env, cwd, lines):
    """Start a process and immediately wire up a background reader thread."""
    proc = start_process(cmd, env, cwd)
    threading.Thread(target=stream_output, args=(proc, lines), daemon=True).start()
    return proc


def kill_all(processes):
    for p in processes:
        p.terminate()
    for p in processes:
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()
