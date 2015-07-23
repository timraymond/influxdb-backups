package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"time"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

var (
	influxHost string = os.Getenv("INFLUXDB_PORT_8808_TCP_ADDR")
	influxPort string = os.Getenv("INFLUXDB_PORT_8808_TCP_PORT")
	s3Bucket   string = os.Getenv("AWS_BUCKET")
	letters           = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func main() {
	auth, err := aws.EnvAuth()
	if err != nil {
		log.Fatal(err)
	}

	client := s3.New(auth, aws.USEast)
	backupBucket := client.Bucket(s3Bucket)

	backup, err := triggerBackup()
	if err != nil {
		panic("something went wrong")
	}
	info, _ := backup.Stat()
	backupBucket.PutReader("influx-"+randName(10), backup, info.Size(), "application/octet-stream", s3.PublicRead)

	err = os.Remove(info.Name())
	if err != nil {
		log.Fatal(err)
	}
}

// randName returns a random string of letters of a given length
func randName(n int) string {
	rand.Seed(time.Now().UTC().UnixNano())
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// triggerBackup invokes the influxd command in backup mode, returning a handle
// to the backup file
func triggerBackup() (backup *os.File, err error) {
	influxBackup := exec.Command("influxd", "backup", "-host", influxHost+":"+influxPort, "influx-snapshot")

	out, err := influxBackup.Output()
	if err != nil {
		log.Println("Backup failed to execute. Error was:", err.Error())
		fmt.Println(string(out))
		return
	}

	backup, err = os.Open("influx-snapshot")
	if err != nil {
		log.Println("Backup file missing despite successful backup. Error was:", err.Error())
		return
	}
	return
}
