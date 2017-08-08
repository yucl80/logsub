package main

import (
	"github.com/go-redis/redis"
	"fmt"
	"time"
	"os"
	"os/signal"
	"syscall"
	"encoding/json"
	"strings"
	"regexp"
	"log"
	"bufio"
)

var (
	datePattern = regexp.MustCompile("(\\d{4}-\\d{2}-\\d{2})")
	redisUri    = "10.62.14.40:6379"
	topics      = ""
	console     = ""
)

func init() {
	file, err := os.Open("sub.conf")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "redis") {
			redisUri = strings.Split(line, "=")[1]
		} else if strings.HasPrefix(line, "topics") {
			topics = strings.Split(line, "=")[1]
		} else  if strings.HasPrefix(line, "console") {
			console = strings.Split(line, "=")[1]
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

}

func main() {
	initSysSignal()

	client := redis.NewClient(&redis.Options{
		Addr:     redisUri,
		Password: "",
		DB:       0,
	})
	pubsub := client.Subscribe(strings.Split(topics, ",") ...)

	defer pubsub.Close()

	for {
		msgi, err := pubsub.ReceiveTimeout(time.Minute)
		if err != nil {
			fmt.Printf("%sn", err.Error())
		}
		switch msg := msgi.(type) {
		case *redis.Subscription:
			fmt.Println("subscribed to", msg.Channel)
		case *redis.Message:
			go writeDataToFile(msg)
		default:
			fmt.Println("unreached")
		}
	}

}

func writeDataToFile(msg *redis.Message) {
	//fmt.Println("received", msg.Payload, "from", msg.Channel)
	var objmap map[string]*string
	err := json.Unmarshal([]byte(msg.Payload), &objmap)
	if (err != nil) {
		fmt.Printf("%s\n", err.Error())
	} else {
		stack := *objmap["stack"]
		service := *objmap["service"]
		index := *objmap["index"]
		message := *objmap["message"]
		path := *objmap["path"]
		shortFilename := path[strings.LastIndex(string(path), "/")+1:]
		date := datePattern.FindString(message)
		if strings.HasSuffix(shortFilename,"-json.log") {
			shortFilename = "console.out"
			var objmap1 map[string]*string
			json.Unmarshal([]byte(message), &objmap1)
			message = *objmap1["log"]
			time,_ :=time.Parse("2006-01-02T15:04:05.999999Z",*objmap1["time"])
			date =time.Local().Format("2006-01-02")
		}
		if console != "" {
			for _, token := range strings.Split(console,",") {
				if strings.Contains(shortFilename,token) {
					fmt.Printf("%s\n",message)
					break
				}
			}
		}
		if datePattern.MatchString(shortFilename) {
			shortFilename = service + "-" + index + "." + shortFilename
		} else {
			shortFilename = service + "-" + index + "." + shortFilename + "." + date
		}
		fullFileName := stack + "/" + service + "/"+shortFilename
		dir := fullFileName[0:strings.LastIndex(fullFileName,"/")]
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.MkdirAll(dir, 0755)
		}
		f, err := os.OpenFile(fullFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0755)
		if err != nil {
			fmt.Printf("%s\n",err.Error())
		}
		defer f.Close()
		if _, err = f.WriteString(message); err != nil {
			fmt.Printf("%s\n",err.Error())
		}



	}

}

func initSysSignal() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
		syscall.SIGKILL,
	)

	go func() {
		sig := <-sc
		fmt.Printf("receive signal [%d] to exit", sig)
		os.Exit(0)
	}()
}
