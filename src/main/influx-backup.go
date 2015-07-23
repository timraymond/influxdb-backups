package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"time"

	"github.com/mitchellh/goamz/aws"
	"github.com/mitchellh/goamz/s3"
)

var (
	influxHost string = os.Getenv("INFLUXDB_PORT_8808_TCP_ADDR")
	influxPort string = os.Getenv("INFLUXDB_PORT_8808_TCP_PORT")
	s3Bucket   string = os.Getenv("AWS_BUCKET")
	freqStr    string = os.Getenv("BACKUP_FREQUENCY")
	letters           = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
)

func main() {
	backupFrequency, err := time.ParseDuration(freqStr)
	if err != nil {
		log.Fatalln("Error parsing backup frequency:", err.Error())
	}

	stop := make(chan struct{})
	log.Printf("Backups will be taken every %s\n", backupFrequency)
	ticker := time.NewTicker(backupFrequency)
	go func() {
		log.Println("Ready")
		for {
			select {
			case <-ticker.C:
				log.Println("Starting backup...")
				backup, err := captureBackup()
				if err != nil {
					log.Fatal(err)
				}

				log.Println("Uploading backup to S3...")
				err = s3Upload("influx-"+randName(10), s3Bucket, backup)

				if err != nil {
					log.Fatal(err)
				}

				log.Println("Cleaning up...")
				info, _ := backup.Stat()
				err = os.Remove(info.Name())
				if err != nil {
					log.Fatal(err)
				}
				log.Println("Backup Complete")
			case <-stop:
				ticker.Stop()
				return
			}
		}
	}()

	for {
		time.Sleep(10 * time.Second)
		runtime.Gosched()
	}
}

func s3Upload(uploadName string, bucketName string, content *os.File) error {
	auth, err := aws.EnvAuth()
	if err != nil {
		return err
	}

	client := s3.New(auth, aws.USEast)
	bucket := client.Bucket(bucketName)

	info, err := content.Stat()
	if err != nil {
		return err
	}
	bucket.PutReader(uploadName, content, info.Size(), "application/octet-stream", s3.PublicRead)
	return nil
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

// captureBackup invokes the influxd command in backup mode, returning a handle
// to the backup file
func captureBackup() (backup *os.File, err error) {
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
