package tjouzijie_pipeline

import (
    "database/sql"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "github.com/hu17889/go_spider/core/common/com_interfaces"
    "github.com/hu17889/go_spider/core/common/config"
    "github.com/hu17889/go_spider/core/common/mlog"
    "github.com/hu17889/go_spider/core/common/page_items"
    "github.com/hu17889/go_spider/core/common/util"
    "regexp"
    "strconv"
    "strings"
)

type MyPipeline struct {
}

func NewMyPipeline() *MyPipeline {
    return &MyPipeline{}
}

func (this *MyPipeline) rmunit(money string) float64 {
    regDetail, _ := regexp.Compile("([0-9\\.]+)(.*)")
    matchstrs := regDetail.FindStringSubmatch(money)
    if len(matchstrs) == 3 {
        num, err := strconv.ParseFloat(matchstrs[1], 32)
        if err != nil {
            mlog.LogInst().LogError("rmunit change string to int : " + matchstrs[1])
            return 0
        }
        switch matchstrs[2] {
        case "亿":
            return num * 100000000
        case "千万":
            return num * 10000000
        case "百万":
            return num * 1000000
        case "万":
            return num * 10000
        case "千":
            return num * 1000
        }
    }
    return 0
}

func (this *MyPipeline) moneyToNum(money string) float64 {
    regDetail, _ := regexp.Compile("^RMB (.*)")
    matchstrs := regDetail.FindStringSubmatch(money)
    if len(matchstrs) == 2 {
        return this.rmunit(matchstrs[1])
    }
    regDetail, _ = regexp.Compile("^USD (.*)")
    matchstrs = regDetail.FindStringSubmatch(money)
    if len(matchstrs) == 2 {
        return 6 * this.rmunit(matchstrs[1])
    }
    return 0
}

func (this *MyPipeline) Process(items *page_items.PageItems, t com_interfaces.Task) {
    // get mysql config
    configPath := util.GetWDPath() + "/etc/mysql.conf"
    conf := config.NewConfig().Load(configPath)

    // init mysql
    user := conf.GlobalGet("user")
    pwd := conf.GlobalGet("pwd")
    db, err := sql.Open("mysql", user+":"+pwd+"@/touzi")
    if err != nil {
        mlog.LogInst().LogError("mysql connect failed")
    }
    defer db.Close()

    // process
    url := items.GetRequest().GetUrl()
    regDetail, _ := regexp.Compile("^http://zdb.pedaily.cn/inv/\\d+/$")
    if regDetail.MatchString(url) {
        // 列表页
        // load data
        result := make(map[string]map[string]string)
        for key, value := range items.GetAll() {
            parts := strings.Split(key, "\t")
            if len(parts) != 2 {
                continue
            }

            eid := parts[0]
            keyname := parts[1]

            if result[eid] == nil {
                result[eid] = make(map[string]string)
            }
            result[eid][keyname] = value
        }
        for eid, data := range result {
            row, err := db.Prepare("SELECT eid FROM touzi.invest_event WHERE eid = ?")
            if err != nil {
                mlog.LogInst().LogError("SELECT eid FROM touzi.invest_event WHERE eid = " + eid + "\t" + err.Error())
                continue
            }
            defer row.Close()
            if len(data) != 7 {
                errorinfo := fmt.Sprintf("列表页数据行未抓全：eid=%s : %v", eid, data)
                mlog.LogInst().LogError(errorinfo)
                continue
            }

            moneynum := this.moneyToNum(data["money"])
            var t int
            if row.QueryRow(eid).Scan(&t) != nil {
                // event exist
                stmtIns, err := db.Prepare("INSERT touzi.invest_event SET eid=?, receiver=?, invester=?, industryid=?, industry=?, money=?, moneynum=?, time=?")
                if err != nil {
                    mlog.LogInst().LogError("INSERT touzi.invest_event SET eid=?, receiver=?, invester=?, industryid=?, industry=?, money=?, moneynum=?, time=?" + err.Error())
                    continue
                }
                defer stmtIns.Close()

                _, err = stmtIns.Exec(data["eid"], data["receiver"], data["invester"], data["industryid"], data["industry"], data["money"], moneynum, data["time"])
                if err != nil {
                    errorinfo := fmt.Sprintf("插入数据exec执行错误：eid=%s : %v", eid, data)
                    mlog.LogInst().LogError(errorinfo)
                    continue
                }
            } else {
                // event not exist
                stmtIns, err := db.Prepare("UPDATE  touzi.invest_event SET eid=?, receiver=?, invester=?, industryid=?, industry=?, money=?, moneynum=?, time=? where eid=?")
                if err != nil {
                    mlog.LogInst().LogError("UPDATE  touzi.invest_event SET eid=?, receiver=?, invester=?, industryid=?, industry=?, money=?, moneynum=?, time=? where eid=?" + err.Error())
                    continue
                }
                defer stmtIns.Close()

                _, err = stmtIns.Exec(data["eid"], data["receiver"], data["invester"], data["industryid"], data["industry"], data["money"], moneynum, data["time"], eid)
                if err != nil {
                    errorinfo := fmt.Sprintf("更新数据exec执行错误：eid=%s : %v", eid, data)
                    mlog.LogInst().LogError(errorinfo)
                    continue
                }
            }
        }

    }

    regDetail, _ = regexp.Compile("^http://zdb.pedaily.cn/inv/show(\\d+)/$")
    if regDetail.MatchString(url) {
        // 详情页
        eid, ok := items.GetItem("eid")
        if !ok {
            mlog.LogInst().LogError("http://zdb.pedaily.cn/inv/show has no eid")
            return
        }
        lun, ok := items.GetItem("lun")
        if !ok {
            mlog.LogInst().LogError("http://zdb.pedaily.cn/inv/show has no lun")
            return
        }
        desc, ok := items.GetItem("desc")
        if !ok {
            mlog.LogInst().LogError("http://zdb.pedaily.cn/inv/show has no desc")
            return
        }

        row, err := db.Prepare("SELECT eid FROM touzi.invest_event WHERE eid = ?")
        if err != nil {
            mlog.LogInst().LogError("SELECT eid FROM touzi.invest_event WHERE eid = " + eid + "\t" + err.Error())
            return
        }
        defer row.Close()

        var t int
        if row.QueryRow(eid).Scan(&t) != nil {
            // event exist
            stmtIns, err := db.Prepare("INSERT touzi.invest_event SET eid=?, lun=?, `desc`=?")
            if err != nil {
                mlog.LogInst().LogError("INSERT touzi.invest_event SET eid=?, lun=?, `desc`=?" + err.Error())
                return
            }
            defer stmtIns.Close()

            _, err = stmtIns.Exec(eid, lun, desc)
            if err != nil {
                errorinfo := fmt.Sprintf("插入数据exec执行错误：eid=%s, lun=%s, desc=%s", eid, lun, desc)
                mlog.LogInst().LogError(errorinfo)
                return
            }
        } else {
            // event not exist
            stmtIns, err := db.Prepare("UPDATE  touzi.invest_event SET lun=?, `desc`=? where eid=?")
            if err != nil {
                mlog.LogInst().LogError("UPDATE  touzi.invest_event SET lun=?, `desc`=? where eid=?" + err.Error())
                return
            }
            defer stmtIns.Close()

            _, err = stmtIns.Exec(lun, desc, eid)
            if err != nil {
                errorinfo := fmt.Sprintf("更新数据exec执行错误：eid=%s, lun=%s, desc=%s", eid, lun, desc)
                mlog.LogInst().LogError(errorinfo)
                return
            }
        }
    }

}
