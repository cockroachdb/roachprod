package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	awsAccessKeyIDKey     = "AWS_ACCESS_KEY_ID"
	awsSecretAccessKeyKey = "AWS_SECRET_ACCESS_KEY"
)

func upload(args []string) error {
	backend := "s3"
	switch n := len(args); n {
	case 0:
		return fmt.Errorf("no test directory specified")
	case 1:
		// Default to S3 backend.
	case 2:
		backend = args[1]
	default:
		return fmt.Errorf("too many arguments (expected 2): %s", args)
	}

	testdir := args[0]

	if backend != "s3" {
		return fmt.Errorf("invalid backend %s (valid backends: s3)", backend)
	}

	if _, ok := os.LookupEnv(awsAccessKeyIDKey); !ok {
		log.Fatalf("AWS access key ID environment variable %s is not set", awsAccessKeyIDKey)
	}
	if _, ok := os.LookupEnv(awsSecretAccessKeyKey); !ok {
		log.Fatalf("AWS secret access key environment variable %s is not set", awsSecretAccessKeyKey)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})
	if err != nil {
		log.Fatalf("AWS session: %s", err)
	}
	svc := s3.New(sess)

	bucketName := "cockroachlabs"

	tarpath := testdir + ".tgz"
	cmd := exec.Command("tar", "-czf", tarpath, testdir)
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s: %s", cmd, err)
	}

	f, err := os.Open(tarpath)
	if err != nil {
		log.Fatalf("os.Open(%s): %s", tarpath, err)
	}

	key := "roachperf/" + tarpath

	putObjectInput := s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   f,
	}
	if _, err := svc.PutObject(&putObjectInput); err != nil {
		log.Fatalf("s3 upload %s: %s", tarpath, err)
	}
	fmt.Printf("Uploaded %s to %s/%s\n", tarpath, bucketName, key)

	if err := f.Close(); err != nil {
		log.Fatal(err)
	}

	if err := os.Remove(tarpath); err != nil {
		log.Fatal(err)
	}
	return nil
}
