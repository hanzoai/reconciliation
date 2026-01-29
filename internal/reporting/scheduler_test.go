package reporting

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseSchedule(t *testing.T) {
	t.Run("valid cron expressions", func(t *testing.T) {
		validSchedules := []string{
			"0 6 * * *",   // 6:00 AM daily (default)
			"0 0 * * *",   // midnight daily
			"30 2 * * *",  // 2:30 AM daily
			"0 */2 * * *", // every 2 hours
			"0 6 * * 1",   // 6:00 AM every Monday
			"0 6 1 * *",   // 6:00 AM first day of month
		}

		for _, schedule := range validSchedules {
			err := ParseSchedule(schedule)
			require.NoError(t, err, "schedule '%s' should be valid", schedule)
		}
	})

	t.Run("invalid cron expressions", func(t *testing.T) {
		invalidSchedules := []string{
			"invalid",
			"* * *",          // too few fields
			"60 * * * *",     // minute > 59
			"* 25 * * *",     // hour > 23
			"* * 32 * *",     // day > 31
			"* * * 13 *",     // month > 12
			"* * * * 8",      // weekday > 7
			"",               // empty
			"0 6 * * * *",    // too many fields (seconds not supported)
			"a b c d e",      // non-numeric
			"0 6 * * * 2024", // extra field
		}

		for _, schedule := range invalidSchedules {
			err := ParseSchedule(schedule)
			require.Error(t, err, "schedule '%s' should be invalid", schedule)
		}
	})

	t.Run("default schedule is valid", func(t *testing.T) {
		err := ParseSchedule(DefaultReportSchedule)
		require.NoError(t, err)
		require.Equal(t, "0 6 * * *", DefaultReportSchedule)
	})
}

func TestCalculateDMinusOnePeriod(t *testing.T) {
	t.Run("calculates D-1 period correctly", func(t *testing.T) {
		// Reference time: January 15, 2024 at 10:30:00 UTC
		referenceTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// Expected: D-1 = January 14, 2024
		expectedStart := time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2024, 1, 14, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
	})

	t.Run("handles month boundary", func(t *testing.T) {
		// Reference time: February 1, 2024 at 06:00:00 UTC
		referenceTime := time.Date(2024, 2, 1, 6, 0, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// Expected: D-1 = January 31, 2024
		expectedStart := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2024, 1, 31, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
	})

	t.Run("handles year boundary", func(t *testing.T) {
		// Reference time: January 1, 2024 at 06:00:00 UTC
		referenceTime := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// Expected: D-1 = December 31, 2023
		expectedStart := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2023, 12, 31, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
	})

	t.Run("handles leap year", func(t *testing.T) {
		// Reference time: March 1, 2024 (leap year) at 06:00:00 UTC
		referenceTime := time.Date(2024, 3, 1, 6, 0, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// Expected: D-1 = February 29, 2024 (leap day)
		expectedStart := time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2024, 2, 29, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
	})

	t.Run("handles non-leap year", func(t *testing.T) {
		// Reference time: March 1, 2023 (non-leap year) at 06:00:00 UTC
		referenceTime := time.Date(2023, 3, 1, 6, 0, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// Expected: D-1 = February 28, 2023
		expectedStart := time.Date(2023, 2, 28, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2023, 2, 28, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
	})

	t.Run("converts non-UTC timezone to UTC", func(t *testing.T) {
		// Reference time in a different timezone
		loc, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		referenceTime := time.Date(2024, 1, 15, 10, 30, 0, 0, loc)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// The function should convert to UTC first, so D-1 is based on UTC date
		// January 15, 10:30 AM EST = January 15, 15:30 UTC
		// D-1 in UTC = January 14
		expectedStart := time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC)
		expectedEnd := time.Date(2024, 1, 14, 23, 59, 59, 999999999, time.UTC)

		require.Equal(t, expectedStart, start)
		require.Equal(t, expectedEnd, end)
		require.Equal(t, time.UTC, start.Location())
		require.Equal(t, time.UTC, end.Location())
	})

	t.Run("period spans exactly 24 hours minus 1 nanosecond", func(t *testing.T) {
		referenceTime := time.Date(2024, 1, 15, 6, 0, 0, 0, time.UTC)

		start, end := CalculateDMinusOnePeriod(referenceTime)

		// The period should span from 00:00:00.000000000 to 23:59:59.999999999
		duration := end.Sub(start)
		expectedDuration := 24*time.Hour - time.Nanosecond

		require.Equal(t, expectedDuration, duration)
	})
}

func TestNewScheduler(t *testing.T) {
	t.Run("creates scheduler with default schedule when empty", func(t *testing.T) {
		config := SchedulerConfig{
			Schedule: "",
		}

		scheduler := NewScheduler(nil, nil, config)

		require.NotNil(t, scheduler)
		require.Equal(t, DefaultReportSchedule, scheduler.config.Schedule)
	})

	t.Run("creates scheduler with custom schedule", func(t *testing.T) {
		config := SchedulerConfig{
			Schedule: "0 0 * * *",
		}

		scheduler := NewScheduler(nil, nil, config)

		require.NotNil(t, scheduler)
		require.Equal(t, "0 0 * * *", scheduler.config.Schedule)
	})
}

func TestSchedulerConfig(t *testing.T) {
	t.Run("default schedule constant is correct", func(t *testing.T) {
		// Default should be 6:00 UTC daily
		require.Equal(t, "0 6 * * *", DefaultReportSchedule)
	})
}
