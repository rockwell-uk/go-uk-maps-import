package rates

import (
	"time"
)

func CalcRecordsProcessed(rates []RateInfo) int {
	var total int

	for _, rate := range rates {
		total += rate.Records
	}

	return total
}

func CalcRowsGenerated(rates []RateInfo) int {
	var total int

	for _, rate := range rates {
		for _, r := range rate.Rows {
			total += r
		}
	}

	return total
}

func CalcAvgRate(ratesInfo []RateInfo) int {
	var total float64

	for _, rateInfo := range ratesInfo {
		total += calcRate(rateInfo)
	}

	return int(total / float64(len(ratesInfo)))
}

func CalcMaxRate(ratesInfo []RateInfo) int {
	var maxRate float64

	for _, rateInfo := range ratesInfo {
		var rate float64 = calcRate(rateInfo)
		if rate > maxRate {
			maxRate = calcRate(rateInfo)
		}
	}

	return int(maxRate)
}

func CalcActualRate(ratesInfo []RateInfo, duration time.Duration) int {
	totRecords := CalcRecordsProcessed(ratesInfo)

	return int(float64(totRecords) / float64(duration) * float64(time.Second))
}

func calcRate(rateInfo RateInfo) float64 {
	var records int = rateInfo.Records

	return float64(records) / float64(rateInfo.Duration) * float64(time.Second)
}
