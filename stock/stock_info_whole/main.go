// from sina
package main

import (
    "fmt"
    "github.com/bitly/go-simplejson"
    "github.com/hu17889/data_crawl/stock/stock_info_whole/mypipeline"
    "github.com/hu17889/go_spider/core/common/mlog"
    "github.com/hu17889/go_spider/core/common/page"
    "github.com/hu17889/go_spider/core/pipeline"
    "github.com/hu17889/go_spider/core/scheduler"
    "github.com/hu17889/go_spider/core/spider"
    "regexp"
    "strings"
    //"log"
    "strconv"
)

type MyPageProcesser struct {
    pageid int
}

func NewMyPageProcesser() *MyPageProcesser {
    return &MyPageProcesser{pageid: 1}
}

func (this *MyPageProcesser) saveOneItem(name string, keyname string, symbol string, elem *simplejson.Json, p *page.Page) {
    data, err := elem.Get(name).String()
    if keyname == "amplitude" {
        data = this.cutPercent(data)
    }
    if err != nil {
        //mlog.LogInst().LogError("data=" + data + ": symbol=" + symbol + " : " + name + " is not useful : " + err.Error())
        return
    }
    p.AddField(symbol+"\t"+keyname, data)
}

func (this *MyPageProcesser) dealSinaList(p *page.Page) {
    query := p.GetJson()
    datas := query.Get("data")

    for i := 0; i < 60; i++ {
        elem := datas.GetIndex(i)
        symbol, err := elem.Get("symbol").String()
        if symbol == "" || err != nil {
            mlog.LogInst().LogError(symbol + ": symbol is not useful")
            continue
        }
        p.AddField(symbol+"\tsymbol", symbol)

        this.saveOneItem("name", "name", symbol, elem, p)
        this.saveOneItem("cname", "cname", symbol, elem, p)
        this.saveOneItem("category", "catename_sina", symbol, elem, p)
        this.saveOneItem("category_id", "cateid_sina", symbol, elem, p)
        this.saveOneItem("preclose", "preclose", symbol, elem, p)
        this.saveOneItem("open", "open", symbol, elem, p)
        this.saveOneItem("high", "high_today", symbol, elem, p)
        this.saveOneItem("low", "low_today", symbol, elem, p)
        this.saveOneItem("amplitude", "amplitude", symbol, elem, p)
        this.saveOneItem("volume", "volume", symbol, elem, p)
        this.saveOneItem("mktcap", "mktcap", symbol, elem, p)
        this.saveOneItem("pe", "pe", symbol, elem, p)
        //http://finance.yahoo.com/d/quotes.csv?s=AAPL&f=a2 b4 d e j k m3 m4
        yahoosymbolkey := strings.Replace(symbol, ".", "-", -1)
        sinasymbolkey := strings.Replace(symbol, ".", "$", -1)
        p.AddTargetRequest("http://finance.yahoo.com/d/quotes.csv?s="+yahoosymbolkey+"&f=a2%20b4%20d%20e%20j%20k%20m3%20m4%20s7%20j6%20k5%20m6%m8", "text")
        p.AddTargetRequest("http://hq.sinajs.cn/list=gb_"+strings.ToLower(sinasymbolkey), "text")
    }
    this.pageid++
    p.AddTargetRequest("http://stock.finance.sina.com.cn/usstock/api/jsonp.php/t/US_CategoryService.getList?page="+strconv.Itoa(this.pageid)+"&num=60", "jsonp")
}

// 去除字符串字段中的%等字符转换成浮点数字符串
func (this *MyPageProcesser) cutPercent(str string) string {
    str = strings.Replace(str, "%", "", -1)
    return str
}

func (this *MyPageProcesser) dealYahoo(symbol string, p *page.Page) {
    // 0    a2   Average Daily Volume 3月每日成交平均量
    // 2    b4   Book Value 每股净资产，账面价值
    // 4    d    Dividend/Share   每股股息
    // 6    e    Earnings/Share 摊薄每股收益
    // 8    j    52周最低
    // 10   k    52周最高
    // 12   m3   50日移动平均
    // 14   m4   200日移动平均
    // 16   s7   空头回补天数
    // 18   j6   离52周最低变化率 +1.1%
    // 20   k5   离52周最高变化率 -1.1%
    // 22   m6   +15.47%,Percent Change From 200-day Moving Average
    // 24   m8   +7.82%,Percent Change From 50-day Moving Average
    body := strings.TrimSpace(p.GetBodyStr())
    //println(body)
    parts := strings.Split(body, ",")
    if len(parts) != 25 {
        mlog.LogInst().LogError("Yahoo stock api catch failed; url = " + p.GetRequest().GetUrl() + " symbol = " + symbol + "; ")
        return
    }
    p.AddField(symbol+"\tsymbol", symbol)
    p.AddField(symbol+"\tavg_vol_3m", parts[0])
    p.AddField(symbol+"\tbook_value", parts[2])
    p.AddField(symbol+"\tdivi_share", parts[4])
    p.AddField(symbol+"\teps", parts[6])
    p.AddField(symbol+"\tlow_52", parts[8])
    p.AddField(symbol+"\thigh_52", parts[10])
    p.AddField(symbol+"\tavg_50", parts[12])
    p.AddField(symbol+"\tavg_200", parts[14])
    p.AddField(symbol+"\tshort_ratio", parts[16])
    p.AddField(symbol+"\tchange_rate_low_52", this.cutPercent(parts[18]))
    p.AddField(symbol+"\tchange_rate_high_52", this.cutPercent(parts[20]))
    p.AddField(symbol+"\tchage_rate_avg_200", this.cutPercent(parts[22]))
    p.AddField(symbol+"\tchage_rate_avg_50", this.cutPercent(parts[24]))

}

func (this *MyPageProcesser) dealSinaStockDetial(symbol string, p *page.Page) {
    // 1    当前价格
    // 2    涨跌百分比
    // 11   10平均成交量
    // 22   盘后涨跌百分比
    // 26   盘后价格
    // 27   盘后交易量
    body := strings.TrimSpace(p.GetBodyStr())
    //println(body)
    start := strings.Index(body, "\"")
    end := strings.LastIndex(body, "\"")
    if start >= end || start < 0 {
        mlog.LogInst().LogError("Sina stock detail catch failed; no quotes; url = " + p.GetRequest().GetUrl() + " symbol = " + symbol + "; ")
        return
    }
    body = body[start+1 : end]
    parts := strings.Split(body, ",")
    if len(parts) < 28 {
        mlog.LogInst().LogError("Sina stock detail catch failed; less segments; url = " + p.GetRequest().GetUrl() + " symbol = " + symbol + "; ")
        return
    }
    price, err := strconv.ParseFloat(parts[1], 64)
    changeFrom5 := 0.0
    if err != nil {
        changeFrom5 = 0.0
    } else {
        changeFrom5 = float64((price-5)/5) * 100
    }
    show5 := fmt.Sprintf("%.2f", changeFrom5)

    p.AddField(symbol+"\tsymbol", symbol)
    p.AddField(symbol+"\tprice", parts[1])
    p.AddField(symbol+"\tchange_rate_5", show5)
    p.AddField(symbol+"\tchange_rate", parts[2])
    p.AddField(symbol+"\tavg_vol_10d", parts[11])
    p.AddField(symbol+"\tchange_rate_pre_price", parts[22])
    p.AddField(symbol+"\tpre_price", parts[26])
    p.AddField(symbol+"\tpre_volumn", parts[27])
}

// Parse html dom here and record the parse result that we want to crawl.
// Package simplejson (https://github.com/bitly/go-simplejson) is used to parse data of json.
func (this *MyPageProcesser) Process(p *page.Page) {
    if this.pageid > 145 {
        return
    }

    if !p.IsSucc() {
        println(p.Errormsg())
        return
    }

    url := p.GetRequest().GetUrl()
    body := p.GetBodyStr()
    if body == "" {
        mlog.LogInst().LogError("Get empty body; url = " + url)
    }

    regDetail, _ := regexp.Compile("^http://stock.finance.sina.com.cn/usstock/api/jsonp.php")
    if regDetail.MatchString(url) {
        this.dealSinaList(p)
    }

    regDetail, err := regexp.Compile("^http://finance.yahoo.com/d/quotes.csv\\?s=(.+)&")
    if err != nil {
        mlog.LogInst().LogError("regexp match failed; url = " + p.GetRequest().GetUrl())
    }
    matchstrs := regDetail.FindStringSubmatch(url)
    if len(matchstrs) == 2 {
        yahoosymbolkey := strings.Replace(matchstrs[1], "-", ".", -1)
        this.dealYahoo(yahoosymbolkey, p)
    }

    regDetail, err = regexp.Compile("^http://hq.sinajs.cn/list=gb_(.*)")
    if err != nil {
        mlog.LogInst().LogError("regexp match failed; url = " + p.GetRequest().GetUrl())
    }
    matchstrs = regDetail.FindStringSubmatch(url)
    if len(matchstrs) == 2 {
        sinasymbolkey := strings.Replace(matchstrs[1], "$", ".", -1)
        this.dealSinaStockDetial(strings.ToUpper(sinasymbolkey), p)
    }
    // Add url of next crawl
    //p.AddTargetRequest("http://live.sina.com.cn/zt/api/l/get/finance/globalnews1/index.htm?format=json&id="+nextidstr+"&pagesize=10&dire=f", "json")
    //println(p.GetTargetRequests())

}

func main() {
    // sina stock list
    spider.NewSpider(NewMyPageProcesser(), "stock_info_whole").
        SetScheduler(scheduler.NewQueueScheduler(true)).
        AddPipeline(pipeline.NewPipelineConsole()). // Print result to std output
        AddPipeline(mypipeline.NewMyPipeline()).
        AddUrl("http://stock.finance.sina.com.cn/usstock/api/jsonp.php/t/US_CategoryService.getList?page=1&num=60", "jsonp"). // start url, html is the responce type ("html" or "json" or "jsonp" or "text")
        SetThreadnum(30).
        OpenFileLogDefault().
        //SetSleepTime("rand", 1000, 3000).
        Run()
    //AddPipeline(pipeline.NewPipelineFile("/tmp/tmpfile")). // print result in file
}
