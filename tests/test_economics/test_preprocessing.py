"""Tests for production history preprocessing."""

import pytest

from src.economics.preprocessing import detect_decline_start, filter_outliers


# ---------------------------------------------------------------------------
# detect_decline_start
# ---------------------------------------------------------------------------

class TestDetectDeclineStart:

    def test_single_peak_at_start(self):
        """Classic decline: peak at month 1, then decline."""
        prod = [1000, 800, 650, 530, 430, 350, 280, 230]
        result = detect_decline_start(prod)
        assert result.peak_index == 0
        assert result.peak_value == 1000
        assert len(result.production_trimmed) == 8

    def test_ramp_up_then_decline(self):
        """Well ramps up then declines — peak should be at the top."""
        prod = [200, 400, 700, 1000, 850, 700, 570, 460]
        result = detect_decline_start(prod)
        assert result.peak_index == 3
        assert result.peak_value == 1000
        assert result.production_trimmed[0] == 1000
        assert len(result.production_trimmed) == 5  # months 4-8

    def test_workover_creates_second_peak(self):
        """Well declines, gets workedover, new peak — should pick latest."""
        prod = [1000, 800, 600, 400, 300,   # initial decline
                250, 200, 150,               # continued decline
                700, 900, 750, 600, 480]     # post-workover bump
        result = detect_decline_start(prod)
        # Most recent significant peak should be the workover peak (900)
        assert result.peak_value == 900
        assert result.peak_index == 9
        assert result.production_trimmed[0] == 900

    def test_small_bump_below_threshold_ignored(self):
        """A minor bump below significance threshold should be ignored."""
        prod = [1000, 800, 600, 400, 300,   # initial decline
                250, 200,                     # continued decline
                350, 300, 250, 200]          # small bump (35% of max)
        result = detect_decline_start(prod, significance_threshold=0.50)
        # 350 is only 35% of 1000, below 50% threshold
        # Should anchor at the original peak
        assert result.peak_index == 0
        assert result.peak_value == 1000

    def test_too_few_nonzero_raises(self):
        """Should raise ValueError with < 3 non-zero months."""
        with pytest.raises(ValueError, match="at least 3"):
            detect_decline_start([100, 0, 0, 0])

    def test_nans_handled(self):
        """NaN values should not crash peak detection."""
        prod = [float("nan"), 500, 800, 1000, 750, 600]
        result = detect_decline_start(prod)
        assert result.peak_value == 1000

    def test_smoothing_window_1_no_smoothing(self):
        """With window=1, no smoothing is applied."""
        prod = [100, 1000, 100, 800, 100, 600]  # noisy
        result = detect_decline_start(prod, smoothing_window=1)
        # Should find peaks in the noisy data
        assert result.peak_value > 0

    def test_custom_threshold(self):
        """Lower threshold should accept smaller peaks as significant."""
        prod = [1000, 800, 600, 400, 300, 200,
                350, 300, 250, 200]
        # At 30% threshold, the bump region (indices 6-7) is significant.
        # Smoothing may shift the apparent peak within the bump.
        result = detect_decline_start(prod, significance_threshold=0.30)
        assert result.peak_index in (6, 7)  # anywhere in the bump region
        assert result.peak_index > 5  # must be in the second half


# ---------------------------------------------------------------------------
# filter_outliers
# ---------------------------------------------------------------------------

class TestFilterOutliers:

    def test_clean_decline_no_outliers(self):
        """A smooth decline should produce no outliers."""
        prod = [1000, 850, 720, 610, 520, 440, 370, 315]
        result = filter_outliers(prod)
        assert result.outlier_count == 0
        assert len(result.production_clean) == 8

    def test_shut_in_month_removed(self):
        """A zero month (shut-in) in the middle should be flagged."""
        prod = [1000, 850, 720, 0, 520, 440, 370, 315]
        result = filter_outliers(prod)
        assert 3 in result.outlier_indices
        assert 0 not in result.production_clean
        assert result.outlier_count >= 1

    def test_spike_down_removed(self):
        """A month with production far below trend should be flagged."""
        prod = [1000, 850, 720, 50, 520, 440, 370, 315]
        result = filter_outliers(prod, threshold=0.30)
        # 50 is well below 30% of neighbors (~720, 520)
        assert 3 in result.outlier_indices

    def test_threshold_sensitivity(self):
        """Higher threshold should flag more months."""
        prod = [1000, 850, 720, 400, 520, 440, 370, 315]
        # At 0.30 threshold, 400 is ~55% of median(720,400,520)=520, not flagged
        result_low = filter_outliers(prod, threshold=0.30)
        # At 0.70 threshold, 400 might be flagged
        result_high = filter_outliers(prod, threshold=0.70)
        assert result_high.outlier_count >= result_low.outlier_count

    def test_custom_time_indices_preserved(self):
        """Custom time indices should be filtered consistently."""
        prod = [1000, 850, 0, 610, 520]
        t = [5, 6, 7, 8, 9]
        result = filter_outliers(prod, time_indices=t)
        assert 7 not in result.time_clean  # month 7 had zero production
        assert len(result.time_clean) == len(result.production_clean)

    def test_short_series_no_filtering(self):
        """Series shorter than window should return unchanged."""
        prod = [1000, 0]
        result = filter_outliers(prod, window=3)
        assert result.outlier_count == 0
        assert result.production_clean == [1000, 0]

    def test_nan_values_removed(self):
        """NaN months should be treated as outliers."""
        prod = [1000, 850, float("nan"), 610, 520]
        result = filter_outliers(prod)
        assert 2 in result.outlier_indices
