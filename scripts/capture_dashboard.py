"""Record the live Grafana dashboard during a scripted scenario, as a GIF.

    python scripts/capture_dashboard.py      # against a stack that is already up

The scenario is: nominal production, a breakdown injected on the bottleneck
(the fault alert fires), recovery, the gateway killed (the timeline shows NO
DATA and the staleness alert fires) and restarted (the gap is filled from the
OPC UA history). Frames are real screenshots taken with Playwright; the only
thing added is the caption strip under each frame.
"""

from __future__ import annotations

import io
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont
from playwright.sync_api import sync_playwright

import compose_scenario
from usine40.ctl import inject_fault

DASHBOARD_URL = (
    f"{compose_scenario.GRAFANA_URL}/d/usine40-cell/usine40-cell"
    "?orgId=1&kiosk&refresh=2s&from=now-5m&to=now&var-station=machining"
)
VIEWPORT = {"width": 1280, "height": 840}
OUTPUT_WIDTH = 900
CAPTION_HEIGHT = 36
FRAME_PERIOD_S = 3.0
FRAME_MS = 700
LAST_FRAME_MS = 2500
PALETTE_COLORS = 96
BREAKDOWN_S = 25.0
BACKGROUND = (17, 18, 23)
FOREGROUND = (236, 236, 232)
MUTED = (150, 150, 144)


@dataclass(frozen=True, slots=True)
class Step:
    caption: str
    duration_s: float
    action: Callable[[], object] | None = None


def _steps() -> tuple[Step, ...]:
    def break_down() -> None:
        url = compose_scenario.OPCUA_URL
        compose_scenario.run_async(inject_fault(url, "machining", BREAKDOWN_S))

    return (
        Step("1/5  Nominal production", 21.0),
        Step("2/5  Breakdown injected on machining: the fault alert fires", 36.0, break_down),
        Step("3/5  Repaired: production resumes", 15.0),
        Step(
            "4/5  Gateway killed: the data goes stale, no state is invented",
            30.0,
            lambda: compose_scenario.compose("kill", "gateway"),
        ),
        Step(
            "5/5  Gateway restarted: the gap is replayed from the OPC UA history",
            27.0,
            lambda: compose_scenario.compose("start", "gateway"),
        ),
    )


def _frame(screenshot: bytes, caption: str, elapsed_s: float) -> Image.Image:
    shot = Image.open(io.BytesIO(screenshot)).convert("RGB")
    height = round(shot.height * OUTPUT_WIDTH / shot.width)
    shot = shot.resize((OUTPUT_WIDTH, height), Image.Resampling.LANCZOS)
    frame = Image.new("RGB", (OUTPUT_WIDTH, height + CAPTION_HEIGHT), BACKGROUND)
    frame.paste(shot, (0, 0))
    draw = ImageDraw.Draw(frame)
    font = ImageFont.load_default(size=17)
    draw.text((12, height + 8), caption, font=font, fill=FOREGROUND)
    clock = f"t = {elapsed_s:3.0f} s"
    width = draw.textlength(clock, font=font)
    draw.text((OUTPUT_WIDTH - width - 12, height + 8), clock, font=font, fill=MUTED)
    return frame


def _save_gif(frames: list[Image.Image], path: Path) -> None:
    """Quantize every frame to one shared palette so that unchanged pixels compress away."""
    sample = frames[:: max(len(frames) // 6, 1)]
    mosaic = Image.new("RGB", (OUTPUT_WIDTH, sum(frame.height for frame in sample)))
    offset = 0
    for frame in sample:
        mosaic.paste(frame, (0, offset))
        offset += frame.height
    palette = mosaic.quantize(colors=PALETTE_COLORS, method=Image.Quantize.MEDIANCUT)
    indexed = [frame.quantize(palette=palette, dither=Image.Dither.NONE) for frame in frames]
    durations = [FRAME_MS] * (len(indexed) - 1) + [LAST_FRAME_MS]
    path.parent.mkdir(parents=True, exist_ok=True)
    indexed[0].save(
        path, save_all=True, append_images=indexed[1:], duration=durations, loop=0, optimize=True
    )


def capture(stack: compose_scenario.Stack, path: Path) -> dict:
    """Play the scenario on the running stack and write the GIF to ``path``."""
    del stack  # the scenario only needs the stack to be up; kept for a uniform phase signature
    frames: list[Image.Image] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        page = browser.new_page(viewport=VIEWPORT)
        page.goto(DASHBOARD_URL, wait_until="networkidle")
        page.wait_for_timeout(4000)
        started = time.monotonic()
        for step in _steps():
            step_started = time.monotonic()
            if step.action is not None:
                step.action()
            while time.monotonic() - step_started < step.duration_s:
                elapsed = time.monotonic() - started
                frames.append(_frame(page.screenshot(), step.caption, elapsed))
                next_frame = started + len(frames) * FRAME_PERIOD_S
                page.wait_for_timeout(max(next_frame - time.monotonic(), 0.0) * 1000.0)
        browser.close()
    _save_gif(frames, path)
    return {
        "frames": len(frames),
        "width": frames[0].width,
        "height": frames[0].height,
        "bytes": path.stat().st_size,
    }


if __name__ == "__main__":
    target = Path(__file__).resolve().parents[1] / "docs" / "dashboard.gif"
    print(capture(compose_scenario.Stack(), target))
