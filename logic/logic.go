package logic

import (
	"context"
	"github.com/Katherine-988/tools"
	jsoniter "github.com/json-iterator/go"
	"log"
	"time"
)

var MAX_TIME = 3
var MAX_DELAY_SECONDS = int64(120)

type Task struct {
	TraceID        string
	OptionType     int32
	UserID         string
	ProductID      int32
	RetryTimes     int
	WriteTimestamp int64
	NeedFeedback   bool
}

func Report() {
	//todo 上报监控
}

func SendMsg() error {
	//todo 发送用户反馈信息
	return nil
}
func ProcessTaskCore(ctx context.Context, task *Task) error {
	err := tools.DBMgr.DB.Table("subscription_tab").Where("user_id = ? and product_id = ?", task.UserID, task.ProductID).Update("status", task.OptionType).Error
	if err != nil {
		log.Println("update failed:", task)
		return err
	}

	if task.NeedFeedback {
		ui := &UserInfo{}
		err := tools.DBMgr.DB.Table("user_info_tab").Where("user_id = ?", task.UserID).Find(ui).Error
		if err != nil {
			log.Println("find user info failed:", task)
			return err
		}
		return SendMsg()
	}
	return nil
}

func ProcessTask(ctx context.Context, input []byte) error {
	task := Task{}
	err := jsoniter.Unmarshal(input, &task)
	if err != nil {
		Report()
		tools.Errorln("internal err:", err)
		return err
	}

	now := time.Now().Unix()
	if now-task.WriteTimestamp >= MAX_DELAY_SECONDS {
		tools.Errorln("msg delay beyond max:", now-task.WriteTimestamp)
		Report()
	}

	err = ProcessTaskCore(ctx, &task)
	if err == nil {
		tools.Infoln("process success:", task)
		Report()
		return nil
	}

	for task.RetryTimes+1 <= MAX_TIME {
		//写到kafka的重试队列
		task.RetryTimes++
		task.WriteTimestamp = time.Now().Unix()
		err := tools.KafkaMgr.Write(ctx, "retry_task_topic", task)
		if err != nil {
			Report()
			tools.Errorln("write retry kafka err:", task)
		} else {
			break
		}
	}

	if task.RetryTimes+1 > MAX_TIME {
		//已经达到最后的重试
		tools.DBMgr.DB.Table("fail_task_tab").Save("具体错误信息") //todo 保存错误数据
		Report()
	}

	return nil
}
