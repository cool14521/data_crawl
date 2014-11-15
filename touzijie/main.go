//
package main

/*
Packages must be imported:
    "core/common/page"
    "core/spider"
Pckages may be imported:
    "core/pipeline": scawler result persistent;
    "github.com/PuerkitoBio/goquery": html dom parser.
*/
import (
    "fmt"
    "github.com/PuerkitoBio/goquery"
    "github.com/hu17889/data_crawl/touzijie/tjouzijie_pipeline"
    "github.com/hu17889/go_spider/core/common/page"
    "github.com/hu17889/go_spider/core/pipeline"
    "github.com/hu17889/go_spider/core/spider"
    "regexp"
    "strings"
)

type MyPageProcesser struct {
    pagelable int
}

func NewMyPageProcesser() *MyPageProcesser {
    return &MyPageProcesser{pagelable: 2}
}

// Parse html dom here and record the parse result that we want to Page.
// Package goquery (http://godoc.org/github.com/PuerkitoBio/goquery) is used to parse html.
func (this *MyPageProcesser) Process(p *page.Page) {
    if this.pagelable >= 44 {
        return
    }

    if !p.IsSucc() {
        println(p.Errormsg())
        return
    }

    query := p.GetHtmlParser()
    url := p.GetRequest().GetUrl()
    regDetail, _ := regexp.Compile("^http://zdb.pedaily.cn/inv/\\d+/$")
    if regDetail.MatchString(url) {
        // 列表页
        query.Find("table[class='zdb_table'] tr").Each(func(i int, s *goquery.Selection) {
            result := make(map[string]string)
            s.Find("td").Each(func(j int, s1 *goquery.Selection) {
                switch j {
                case 0:
                    // 受资方
                    content := s1.Find("a").Text()
                    result["receiver"] = strings.TrimSpace(content)
                case 1:
                    // 投资方
                    content := s1.Text()
                    result["invester"] = strings.TrimSpace(content)
                case 2:
                    // 所属行业
                    content := s1.Find("a").Text()
                    result["industry"] = strings.TrimSpace(content)

                    reg, _ := regexp.Compile("^/inv/h([\\d]+)/$")
                    if str, ok := s1.Find("a").Attr("href"); ok {
                        content := reg.FindStringSubmatch(str)
                        result["industryid"] = strings.TrimSpace(content[1])
                    }
                case 3:
                    // 投资金额
                    content := s1.Text()
                    result["money"] = strings.TrimSpace(content)
                case 4:
                    // 投资时间
                    content := s1.Text()
                    result["time"] = strings.TrimSpace(content)
                case 5:
                    // 投资事件源id
                    reg, _ := regexp.Compile("^/inv/show([\\d]+)/$")
                    if str, ok := s1.Find("a").Attr("href"); ok {
                        content := reg.FindStringSubmatch(str)
                        result["eid"] = strings.TrimSpace(content[1])
                    }
                }
            })
            if _, ok := result["eid"]; !ok {
                return
            }
            for key, value := range result {
                p.AddField(result["eid"]+"\t"+key, value)
            }
            //urls = append(urls, "http://github.com/"+href)
            p.AddTargetRequest("http://zdb.pedaily.cn/inv/show"+result["eid"]+"/", "html")
        })
        nexturl := fmt.Sprintf("http://zdb.pedaily.cn/inv/%v/", this.pagelable)
        p.AddTargetRequest(nexturl, "html")
        this.pagelable++

    }

    regDetail, _ = regexp.Compile("^http://zdb.pedaily.cn/inv/show(\\d+)/$")
    matchstrs := regDetail.FindStringSubmatch(url)
    if len(matchstrs) == 2 {
        // 详情页
        p.AddField("eid", matchstrs[1])
        lun := query.Find(".zdbdata .content p").Eq(3).Contents().Eq(1).Text()
        p.AddField("lun", lun)
        desc := strings.TrimSpace(query.Find(".zdbdata .content p").Eq(7).Text())
        p.AddField("desc", desc)
    }
    // these urls will be saved and crawed by other coroutines.
    /*
       p.AddTargetRequests(urls, "html")

       name := query.Find(".entry-title .author").Text()
       name = strings.Trim(name, " \t\n")
       repository := query.Find(".entry-title .js-current-repository").Text()
       repository = strings.Trim(repository, " \t\n")
       //readme, _ := query.Find("#readme").Html()
       if name == "" {
           p.SetSkip(true)
       }
       // the entity we want to save by Pipeline
       p.AddField("author", name)
       p.AddField("project", repository)
       //p.AddField("readme", readme)
    */
}

func main() {
    // Spider input:
    //  PageProcesser ;
    //  Task name used in Pipeline for record;
    spider.NewSpider(NewMyPageProcesser(), "TaskName").
        AddUrl("http://zdb.pedaily.cn/inv/11/", "html"). // Start url, html is the responce type ("html" or "json")
        //AddUrl("http://zdb.pedaily.cn/inv/show14091/", "html"). // Start url, html is the responce type ("html" or "json")
        AddPipeline(pipeline.NewPipelineConsole()).      // Print result on screen
        AddPipeline(tjouzijie_pipeline.NewMyPipeline()). // Print result on screen
        SetThreadnum(1).                                 // Crawl request by three Coroutines
        OpenFileLogDefault().                            // Open file log with default file path like "WD/log/log.2014-9-1"
        Run()
}
