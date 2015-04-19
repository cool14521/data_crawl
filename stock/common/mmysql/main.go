package mmysql

import (
    "database/sql"
    "fmt"
    _ "github.com/go-sql-driver/mysql"
    "github.com/hu17889/go_spider/core/common/mlog"
    "github.com/hu17889/go_spider/core/common/page_items"
    "strings"
)

type mmysql struct {
    db *sql.DB
}

func NewMmysql(user string, pwd string, dbname string) *mmysql {
    db, err := sql.Open("mysql", user+":"+pwd+"@/"+dbname)
    if err != nil {
        mlog.LogInst().LogError("mysql connect failed")
        return nil
    }
    return &mmysql{db: db}
}

func (this *mmysql) Close() {
    this.db.Close()
}

// save data eid\tkeyname value, eid是where条件，keyname是字段名，value是字段内容
func (this *mmysql) SaveMultiPageItems(items *page_items.PageItems, tablename string) {
    // get data
    result := make(map[string]map[string]string)
    insertsqls := make(map[string]string)
    updatesqls := make(map[string]string)
    for key, value := range items.GetAll() {
        //println(key+value)
        parts := strings.Split(key, "\t")
        if len(parts) != 2 {
            continue
        }

        uniqueId := parts[0]
        keyname := parts[1]

        if result[uniqueId] == nil {
            insertsqls[uniqueId] = "INSERT into " + tablename + " SET "
            updatesqls[uniqueId] = ""
            result[uniqueId] = make(map[string]string)
        }
        insertsqls[uniqueId] += "`" + keyname + "`" + "='" + value + "',"
        updatesqls[uniqueId] += "`" + keyname + "`" + "='" + value + "',"
        result[uniqueId][keyname] = value
    }

    for eid, data := range result {
        sql := insertsqls[eid]
        sql = strings.TrimRight(sql, ",") + " ON DUPLICATE KEY UPDATE " + strings.TrimRight(updatesqls[eid], ",")
        //println(sql)
        stmtIns, err := this.db.Prepare(sql)
        if err != nil {
            errorinfo := fmt.Sprintf("插入或更新Prepare执行错误：eid=%s : %v\n%s\n%s\n", eid, data, sql, err.Error())
            mlog.LogInst().LogError(errorinfo)
            continue
        }
        defer stmtIns.Close()

        _, err = stmtIns.Exec()
        if err != nil {
            errorinfo := fmt.Sprintf("插入或更新exec执行错误：eid=%s : %v\n%s\n%s\n", eid, data, sql, err.Error())
            mlog.LogInst().LogError(errorinfo)
            continue
        }
    }
}
