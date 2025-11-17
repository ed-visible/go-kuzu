package ryu

// #include "ryu.h"
// #include <stdlib.h>
import "C"

import (
	"math"
	"time"
)

// unixEpoch returns the Unix epoch time.
func unixEpoch() time.Time {
	return time.Unix(0, 0)
}

// timeToRyuDate converts a time.Time to a ryu_date_t.
func timeToRyuDate(inputTime time.Time) C.ryu_date_t {
	diff := inputTime.Sub(unixEpoch())
	diffDays := math.Floor(diff.Hours() / 24)
	cRyuDate := C.ryu_date_t{}
	cRyuDate.days = C.int32_t(diffDays)
	return cRyuDate
}

// ryuDateToTime converts a ryu_date_t to a time.Time in UTC.
func ryuDateToTime(cRyuDate C.ryu_date_t) time.Time {
	diff := time.Duration(cRyuDate.days) * 24 * time.Hour
	return unixEpoch().UTC().Add(diff)
}

// timeToRyuTimestamp converts a time.Time to a ryu_timestamp_t.
func timeToRyuTimestamp(inputTime time.Time) C.ryu_timestamp_t {
	nanoseconds := inputTime.UnixNano()
	microseconds := nanoseconds / 1000
	cRyuTime := C.ryu_timestamp_t{}
	cRyuTime.value = C.int64_t(microseconds)
	return cRyuTime
}

// timeToRyuTimestampNs converts a time.Time to a ryu_timestamp_ns_t.
func timeToRyuTimestampNs(inputTime time.Time) C.ryu_timestamp_ns_t {
	nanoseconds := inputTime.UnixNano()
	cRyuTime := C.ryu_timestamp_ns_t{}
	cRyuTime.value = C.int64_t(nanoseconds)
	return cRyuTime
}

// timeHasNanoseconds returns true if the time.Time has non-zero nanoseconds.
func timeHasNanoseconds(inputTime time.Time) bool {
	return inputTime.Nanosecond() != 0
}

// durationToRyuInterval converts a time.Duration to a ryu_interval_t.
func durationToRyuInterval(inputDuration time.Duration) C.ryu_interval_t {
	microseconds := inputDuration.Microseconds()

	cRyuInterval := C.ryu_interval_t{}
	cRyuInterval.micros = C.int64_t(microseconds)
	return cRyuInterval
}

// ryuIntervalToDuration converts a ryu_interval_t to a time.Duration.
func ryuIntervalToDuration(cRyuInterval C.ryu_interval_t) time.Duration {
	days := cRyuInterval.days
	months := cRyuInterval.months
	microseconds := cRyuInterval.micros
	totalDays := int64(days) + int64(months)*30
	totalSeconds := totalDays * 24 * 60 * 60
	totalMicroseconds := totalSeconds*1000000 + int64(microseconds)
	totalNanoseconds := totalMicroseconds * 1000
	return time.Duration(totalNanoseconds)
}
