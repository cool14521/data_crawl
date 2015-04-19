package mtime

import (
    "time"
)

var MTIME_STOCK_SUMMER = true

func IsStockOpen() bool {
    now := time.Now()
    /*
    	year := now.Year()
    	month := now.Month()
    	day := now.Day()
    */
    hour := now.Hour()
    minute := now.Minute()
    weekday := int(now.Weekday())

    h1 := 22
    h2 := 5
    if MTIME_STOCK_SUMMER {
        h1 = 21
        h2 = 4
    }

    if ((hour == h1 && minute >= 30) || (hour > h1 || hour < h2)) && (weekday >= 2 && weekday <= 5) {
        return true
    } else if ((hour == h1 && minute >= 30) || hour > h1) && weekday == 1 {
        return true
    } else if hour < h2 && weekday == 6 {
        return true
    } else {
        return false
    }
}

func IsStockOpenWithPre() bool {
    now := time.Now()
    /*
       year := now.Year()
       month := now.Month()
       day := now.Day()
    */
    hour := now.Hour()
    minute := now.Minute()
    weekday := int(now.Weekday())

    h1 := 21
    h2 := 6
    if MTIME_STOCK_SUMMER {
        h1 = 20
        h2 = 5
    }

    if ((hour == h2 && minute < 30) || (hour > h1 || hour < h2)) && (weekday >= 2 && weekday <= 5) {
        return true
    } else if hour > h1 && weekday == 1 {
        return true
    } else if ((hour == h2 && minute < 30) || hour < h2) && weekday == 6 {
        return true
    } else {
        return false
    }
}
