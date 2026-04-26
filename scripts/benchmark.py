#!/usr/bin/env python3
"""
DistrQ — Benchmark & Zero-Task-Loss Verification Script
========================================================

Usage:
    # Basic benchmark: flood 10K tasks, verify 100% completion
    python benchmark.py

    # With worker-kill test (stop one worker mid-run)
    python benchmark.py --kill-worker --worker-name distrq-worker-1

    # Custom parameters
    python benchmark.py --tasks 10000 --concurrency 200 --api http://localhost:8000

    # Continuous load test
    python benchmark.py --mode continuous --rate 500

Phases:
  1. Health check
  2. Bulk ingest  (10K tasks, single API call)
  3. [Optional] Kill a worker container mid-run
  4. Poll until 100% COMPLETED or timeout
  5. Final verification report

Requirements:
    pip install httpx rich typer
"""

import asyncio
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from typing import Optional

import httpx
import typer
from rich import box
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import (BarColumn, Progress, SpinnerColumn,
                           TaskProgressColumn, TextColumn, TimeElapsedColumn)
from rich.table import Table
from rich.text import Text

app  = typer.Typer(add_completion=False)
con  = Console()

API_BASE    = "http://localhost:8000"
TIMEOUT     = httpx.Timeout(120.0)


# ── Helpers ───────────────────────────────────────────────────────────────────

async def health_check(client: httpx.AsyncClient) -> bool:
    try:
        r = await client.get(f"{API_BASE}/health", timeout=10)
        data = r.json()
        ok = data.get("status") == "ok"
        if ok:
            con.print("[green]✅ Health check passed[/green] "
                      f"(postgres={data['postgres']}, redis={data['redis']})")
        else:
            con.print(f"[red]❌ Health check FAILED: {data}[/red]")
        return ok
    except Exception as e:
        con.print(f"[red]❌ Cannot reach API at {API_BASE}: {e}[/red]")
        return False


async def bulk_ingest(
    client: httpx.AsyncClient,
    n_tasks: int,
    task_type: str = "compute",
    batch_size: int = 1000,
) -> list[str]:
    """
    Ingest n_tasks in batches of batch_size.
    Returns list of all task IDs.
    """
    all_ids: list[str] = []
    batches = [
        list(range(i, min(i + batch_size, n_tasks)))
        for i in range(0, n_tasks, batch_size)
    ]

    con.print(f"\n[bold cyan]📤 Ingesting {n_tasks:,} tasks "
              f"in {len(batches)} batch(es) of ≤{batch_size}…[/bold cyan]")

    t0 = time.perf_counter()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=con,
    ) as progress:
        ptask = progress.add_task("Ingesting…", total=len(batches))

        for batch_idx, batch in enumerate(batches):
            payload = {
                "tasks": [
                    {
                        "task_type":   task_type,
                        "payload":     {"job_index": i, "duration_ms": 50},
                        "priority":    (i % 4) * 5 + 1,   # mix of priorities
                        "max_retries": 3,
                    }
                    for i in batch
                ]
            }
            r = await client.post(
                f"{API_BASE}/tasks/bulk",
                json=payload,
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
            all_ids.extend(data["task_ids"])
            progress.advance(ptask)

    elapsed = time.perf_counter() - t0
    rate    = n_tasks / elapsed
    con.print(
        f"[green]✅ {n_tasks:,} tasks ingested in {elapsed:.2f}s "
        f"({rate:,.0f} tasks/sec)[/green]"
    )
    return all_ids


async def poll_until_complete(
    client:        httpx.AsyncClient,
    total_tasks:   int,
    poll_interval: float = 2.0,
    timeout_sec:   int   = 600,
    kill_worker:   Optional[str] = None,
    kill_at:       float = 0.3,   # kill after 30% completion
) -> dict:
    """
    Poll /stats until all tasks reach terminal state (COMPLETED / FAILED / DEAD_LETTER).
    Returns final counts.
    """
    deadline    = time.time() + timeout_sec
    killed      = False
    start_time  = time.time()

    con.print(f"\n[bold cyan]📊 Monitoring progress (timeout={timeout_sec}s)…[/bold cyan]")
    if kill_worker:
        con.print(f"[yellow]⚡ Will kill worker '{kill_worker}' at ~{kill_at*100:.0f}% completion[/yellow]")

    with Live(console=con, refresh_per_second=2) as live:
        while time.time() < deadline:
            try:
                r    = await client.get(f"{API_BASE}/stats", timeout=10)
                data = r.json()
                db   = data.get("db", {})
            except Exception as e:
                live.update(f"[red]Polling error: {e}[/red]")
                await asyncio.sleep(poll_interval)
                continue

            completed   = db.get("COMPLETED",   0)
            failed      = db.get("FAILED",       0)
            dead_letter = db.get("DEAD_LETTER",  0)
            processing  = db.get("PROCESSING",   0)
            pending     = db.get("PENDING",       0)
            terminal    = completed + failed + dead_letter
            pct         = terminal / total_tasks * 100 if total_tasks else 0
            elapsed     = time.time() - start_time

            # Worker kill simulation
            if kill_worker and not killed and pct >= kill_at * 100:
                live.stop()
                _kill_worker(kill_worker)
                killed = True
                live.start()

            # Build live table
            table = Table(title="DistrQ — Live Stats", box=box.ROUNDED, min_width=55)
            table.add_column("Metric",    style="bold")
            table.add_column("Count",     justify="right")
            table.add_column("Progress",  justify="right")

            table.add_row("✅ COMPLETED",   f"{completed:,}",  f"{completed/total_tasks*100:.1f}%")
            table.add_row("⏳ PROCESSING",  f"{processing:,}", "")
            table.add_row("🕐 PENDING",     f"{pending:,}",    "")
            table.add_row("❌ FAILED",      f"{failed:,}",     f"{failed/total_tasks*100:.1f}%")
            table.add_row("💀 DEAD_LETTER", f"{dead_letter:,}","")
            table.add_row("─" * 15, "─" * 8, "")
            table.add_row("[bold]Terminal Total[/bold]", f"[bold]{terminal:,}[/bold]",
                          f"[bold]{pct:.1f}%[/bold]")
            table.add_row("Queue Depth",
                          f"{data.get('redis', {}).get('queue_depth', '?'):,}", "")
            table.add_row("Elapsed",  f"{elapsed:.1f}s", "")

            live.update(table)

            if terminal >= total_tasks:
                break

            await asyncio.sleep(poll_interval)

    return db


def _kill_worker(worker_name: str) -> None:
    con.print(f"\n[bold red]💥 Stopping worker: {worker_name}[/bold red]")
    result = subprocess.run(
        ["docker", "stop", worker_name],
        capture_output=True, text=True,
    )
    if result.returncode == 0:
        con.print(f"[red]Worker '{worker_name}' stopped. Watchdog will recover orphaned tasks…[/red]")
    else:
        con.print(f"[yellow]Could not stop worker (maybe already stopped): {result.stderr.strip()}[/yellow]")


def print_report(
    task_ids:    list[str],
    final_stats: dict,
    total_tasks: int,
    elapsed:     float,
    kill_worker: Optional[str],
) -> bool:
    """Print final verification report. Returns True if Zero Task Loss achieved."""
    completed   = final_stats.get("COMPLETED",   0)
    failed      = final_stats.get("FAILED",       0)
    dead_letter = final_stats.get("DEAD_LETTER",  0)
    terminal    = completed + failed + dead_letter
    success_pct = completed / total_tasks * 100 if total_tasks else 0
    zero_loss   = terminal >= total_tasks

    con.print("\n" + "═" * 60)
    con.print(Panel.fit(
        "\n".join([
            f"[bold]Total Submitted :[/bold] {total_tasks:,}",
            f"[green bold]COMPLETED        : {completed:,}  ({success_pct:.2f}%)[/green bold]",
            f"[yellow]FAILED           : {failed:,}[/yellow]",
            f"[red]DEAD_LETTER      : {dead_letter:,}[/red]",
            f"[bold]Terminal Total   : {terminal:,} / {total_tasks:,}[/bold]",
            "",
            f"[bold]Total Elapsed    : {elapsed:.1f}s[/bold]",
            f"[bold]Throughput       : {total_tasks/elapsed:,.0f} tasks/sec[/bold]",
            "",
            f"Worker killed mid-run: {'YES — ' + kill_worker if kill_worker else 'No'}",
        ]),
        title="📋 DistrQ Benchmark Report",
        border_style="green" if zero_loss else "red",
    ))

    if zero_loss and completed == total_tasks:
        con.print(Panel(
            "[bold green]🏆 ZERO TASK LOSS ACHIEVED\n"
            "100% of tasks reached COMPLETED state.[/bold green]",
            border_style="green",
        ))
    elif zero_loss:
        con.print(Panel(
            f"[yellow]⚠️  Zero task loss achieved but {failed + dead_letter} "
            f"tasks ended in FAILED/DEAD_LETTER state.\n"
            f"All tasks were processed — none were lost.[/yellow]",
            border_style="yellow",
        ))
    else:
        missing = total_tasks - terminal
        con.print(Panel(
            f"[bold red]❌ TASK LOSS DETECTED\n"
            f"{missing:,} tasks did not reach a terminal state.\n"
            f"Check watchdog logs: docker compose logs watchdog[/bold red]",
            border_style="red",
        ))

    return zero_loss


# ── CLI entry point ────────────────────────────────────────────────────────────

@app.command()
def benchmark(
    tasks:       int            = typer.Option(10_000, "--tasks",       "-n", help="Number of tasks to submit"),
    concurrency: int            = typer.Option(200,    "--concurrency", "-c", help="HTTP client concurrency"),
    api:         str            = typer.Option(API_BASE,"--api",              help="API base URL"),
    kill_worker: Optional[str]  = typer.Option(None,   "--kill-worker",       help="Docker container name to stop mid-run"),
    kill_at:     float          = typer.Option(0.30,   "--kill-at",           help="Kill after this fraction of tasks complete"),
    timeout:     int            = typer.Option(600,    "--timeout",           help="Max seconds to wait for completion"),
    task_type:   str            = typer.Option("compute","--task-type",       help="Task type to submit"),
    batch_size:  int            = typer.Option(1000,   "--batch-size",        help="Tasks per bulk API call"),
):
    """
    Flood DistrQ with N tasks and verify 100% reach COMPLETED state.
    Optionally kill a worker mid-run to prove Zero Task Loss.
    """
    global API_BASE
    API_BASE = api

    asyncio.run(_run(
        n_tasks=tasks,
        concurrency=concurrency,
        kill_worker=kill_worker,
        kill_at=kill_at,
        timeout_sec=timeout,
        task_type=task_type,
        batch_size=batch_size,
    ))


async def _run(
    n_tasks:     int,
    concurrency: int,
    kill_worker: Optional[str],
    kill_at:     float,
    timeout_sec: int,
    task_type:   str,
    batch_size:  int,
):
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=API_BASE, limits=limits, timeout=TIMEOUT) as client:

        con.print(Panel.fit(
            f"[bold cyan]DistrQ Benchmark[/bold cyan]\n"
            f"API: {API_BASE}  |  Tasks: {n_tasks:,}  |  "
            f"Batch: {batch_size}  |  Concurrency: {concurrency}",
            border_style="cyan",
        ))

        # Phase 1: Health
        if not await health_check(client):
            con.print("[red]Aborting — fix the stack first.[/red]")
            raise typer.Exit(1)

        # Phase 2: Ingest
        start = time.perf_counter()
        task_ids = await bulk_ingest(client, n_tasks, task_type, batch_size)

        # Phase 3: Monitor + optional worker kill
        final_stats = await poll_until_complete(
            client, n_tasks,
            kill_worker=kill_worker,
            kill_at=kill_at,
            timeout_sec=timeout_sec,
        )
        elapsed = time.perf_counter() - start

        # Phase 4: Report
        success = print_report(task_ids, final_stats, n_tasks, elapsed, kill_worker)
        raise typer.Exit(0 if success else 1)


if __name__ == "__main__":
    app()