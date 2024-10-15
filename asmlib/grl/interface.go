package grl

import "time"

type (
	GlobalRateLimiter interface {
		Init(
			rate float64,
			burstRatio float64,
			priorities int,
		)
		GetTokens(
			priorityTokenRate map[int32]float64,
		) (int, time.Duration)
		Update(rate float64, burstRatio float64)
	}
)
