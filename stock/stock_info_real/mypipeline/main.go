package mypipeline

import (
    //"fmt"
    _ "github.com/go-sql-driver/mysql"
    "github.com/hu17889/go_spider/core/common/com_interfaces"
    "github.com/hu17889/go_spider/core/common/config"
    //"github.com/hu17889/go_spider/core/common/mlog"
    "github.com/hu17889/data_crawl/stock/common/mmysql"
    "github.com/hu17889/go_spider/core/common/page_items"
    "github.com/hu17889/go_spider/core/common/util"

    "regexp"
    //"strconv"
    //"strings"
)

type MyPipeline struct {
}

func NewMyPipeline() *MyPipeline {
    return &MyPipeline{}
}

func (this *MyPipeline) Process(items *page_items.PageItems, t com_interfaces.Task) {
    // get mysql config
    configPath := util.GetWDPath() + "/etc/mysql.conf"
    conf := config.NewConfig().Load(configPath)

    // init mysql
    user := conf.GlobalGet("user")
    pwd := conf.GlobalGet("pwd")
    dbProcesser := mmysql.NewMmysql(user, pwd, "stockweb")
    defer dbProcesser.Close()

    // process
    url := items.GetRequest().GetUrl()
    regDetail1, _ := regexp.Compile("^http://stock.finance.sina.com.cn/usstock/api/jsonp.php")
    regDetail2, _ := regexp.Compile("^http://finance.yahoo.com/d/quotes.csv\\?s=")
    regDetail3, _ := regexp.Compile("^http://hq.sinajs.cn/list=gb_")
    if regDetail1.MatchString(url) || regDetail2.MatchString(url) || regDetail3.MatchString(url) {
        dbProcesser.SaveMultiPageItems(items, "stock_info_real")

    }
}
