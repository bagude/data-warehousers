"""Production history preprocessing for decline curve analysis.

Two functions that sit upstream of fit_arps():
1. detect_decline_start() — finds the most recent significant peak
2. filter_outliers() — removes anomalous months via rolling median

Both are pure functions operating on production arrays.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class DeclineAnchor:
    """Result of peak detection — where to start the Arps fit."""

    peak_index: int          # 0-based index into original production array
    peak_value: float        # production value at peak
    all_peaks: list[int]     # indices of all detected peaks
    production_trimmed: list[float]  # production from peak onward
    time_trimmed: list[int]  # time indices from peak onward (1-based)


@dataclass(frozen=True)
class FilterResult:
    """Result of outlier filtering."""

    production_clean: list[float]   # filtered production values
    time_clean: list[int]           # corresponding time indices (1-based)
    outlier_indices: list[int]      # 0-based indices of removed months
    outlier_count: int


# ---------------------------------------------------------------------------
# Peak detection
# ---------------------------------------------------------------------------

def detect_decline_start(
    production: list[float],
    significance_threshold: float = 0.50,
    smoothing_window: int = 3,
) -> DeclineAnchor:
    """Find the most recent significant production peak.

    Identifies all local maxima in the (smoothed) production history,
    then selects the latest peak that exceeds a fraction of the global
    maximum. This anchors the Arps decline after the last workover,
    restimulation, or initial ramp-up.

    Parameters
    ----------
    production : list[float]
        Monthly production volumes in chronological order.
    significance_threshold : float
        Fraction of global max a peak must exceed to be "significant"
        (default 0.50 = 50% of all-time max).
    smoothing_window : int
        Rolling average window for noise reduction before peak detection
        (default 3 months). Set to 1 for no smoothing.

    Returns
    -------
    DeclineAnchor with peak location and trimmed production.

    Raises
    ------
    ValueError
        If production has fewer than 3 non-zero values.
    """
    prod = np.array(production, dtype=float)

    # Replace NaN with 0 for processing
    prod = np.where(np.isnan(prod), 0.0, prod)

    nonzero_count = np.count_nonzero(prod)
    if nonzero_count < 3:
        raise ValueError("Need at least 3 non-zero production months.")

    # Smooth to reduce noise (pad edges to avoid boundary distortion)
    if smoothing_window > 1 and len(prod) >= smoothing_window:
        half = smoothing_window // 2
        padded = np.pad(prod, half, mode="edge")
        kernel = np.ones(smoothing_window) / smoothing_window
        smoothed_padded = np.convolve(padded, kernel, mode="valid")
        smoothed = smoothed_padded[: len(prod)]
    else:
        smoothed = prod.copy()

    # Find local maxima, including endpoints
    peaks = []
    n = len(smoothed)

    # Check first element
    if n >= 2 and smoothed[0] >= smoothed[1] and prod[0] > 0:
        peaks.append(0)

    # Check interior points
    for i in range(1, n - 1):
        if smoothed[i] >= smoothed[i - 1] and smoothed[i] >= smoothed[i + 1]:
            if prod[i] > 0:
                peaks.append(i)

    # Check last element
    if n >= 2 and smoothed[-1] >= smoothed[-2] and prod[-1] > 0:
        peaks.append(n - 1)

    # If no peaks found, use the index of the global max
    if not peaks:
        peaks = [int(np.argmax(prod))]

    # Merge nearby peaks: if peaks are within smoothing_window of each
    # other, they're part of the same event — keep the one with the
    # highest original production value.
    merged_peaks: list[int] = []
    group: list[int] = [peaks[0]]
    for i in range(1, len(peaks)):
        if peaks[i] - group[-1] <= smoothing_window:
            group.append(peaks[i])
        else:
            # Pick the highest original value in this group
            best = max(group, key=lambda idx: prod[idx])
            merged_peaks.append(best)
            group = [peaks[i]]
    best = max(group, key=lambda idx: prod[idx])
    merged_peaks.append(best)

    # Global max of original production
    global_max = float(np.max(prod))
    threshold = global_max * significance_threshold

    # Filter to significant peaks
    significant_peaks = [p for p in merged_peaks if prod[p] >= threshold]

    # If no significant peaks, fall back to the global max
    if not significant_peaks:
        significant_peaks = [int(np.argmax(prod))]

    # Take the most recent significant peak
    peak_idx = significant_peaks[-1]
    peak_val = float(prod[peak_idx])

    # Trim production from peak onward
    trimmed = prod[peak_idx:].tolist()
    time_trimmed = list(range(1, len(trimmed) + 1))

    return DeclineAnchor(
        peak_index=peak_idx,
        peak_value=peak_val,
        all_peaks=peaks,
        production_trimmed=trimmed,
        time_trimmed=time_trimmed,
    )


# ---------------------------------------------------------------------------
# Outlier detection
# ---------------------------------------------------------------------------

def filter_outliers(
    production: list[float],
    time_indices: list[int] | None = None,
    window: int = 3,
    threshold: float = 0.30,
) -> FilterResult:
    """Remove anomalous production months using rolling median.

    Flags months where production drops below a fraction of the
    rolling median — catches shut-ins, mechanical failures, and
    data errors without removing legitimate decline.

    Parameters
    ----------
    production : list[float]
        Monthly production volumes in chronological order.
    time_indices : list[int] | None
        Corresponding time indices (1-based). If None, uses 1..N.
    window : int
        Rolling median window size (default 3 months).
    threshold : float
        Fraction of rolling median below which a month is flagged
        as an outlier (default 0.30 = 30%). A month producing less
        than 30% of its local median is considered anomalous.

    Returns
    -------
    FilterResult with clean production and list of removed indices.
    """
    prod = np.array(production, dtype=float)

    if time_indices is None:
        t_idx = list(range(1, len(prod) + 1))
    else:
        t_idx = list(time_indices)

    if len(prod) < window:
        # Too short to filter meaningfully
        return FilterResult(
            production_clean=prod.tolist(),
            time_clean=t_idx,
            outlier_indices=[],
            outlier_count=0,
        )

    # Calculate rolling median using a centered window
    half_w = window // 2
    rolling_median = np.full(len(prod), np.nan)

    for i in range(len(prod)):
        start = max(0, i - half_w)
        end = min(len(prod), i + half_w + 1)
        window_vals = prod[start:end]
        # Use non-zero values for median to avoid shut-in bias
        nonzero = window_vals[window_vals > 0]
        if len(nonzero) > 0:
            rolling_median[i] = np.median(nonzero)

    # Identify outliers: production < threshold * rolling_median
    outlier_mask = np.zeros(len(prod), dtype=bool)
    for i in range(len(prod)):
        if np.isnan(rolling_median[i]):
            continue
        if rolling_median[i] <= 0:
            continue
        # Flag if production is below threshold of local median
        # Also flag NaN and zero months (shut-ins)
        if np.isnan(prod[i]) or prod[i] < threshold * rolling_median[i]:
            outlier_mask[i] = True

    outlier_indices = list(np.where(outlier_mask)[0])

    # Build clean arrays
    clean_mask = ~outlier_mask
    production_clean = prod[clean_mask].tolist()
    time_clean = [t_idx[i] for i in range(len(t_idx)) if clean_mask[i]]

    return FilterResult(
        production_clean=production_clean,
        time_clean=time_clean,
        outlier_indices=outlier_indices,
        outlier_count=int(np.sum(outlier_mask)),
    )
