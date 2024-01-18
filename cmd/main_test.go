package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// HOW TO RUN: go test -v ./... -run Test_FullSuite
//
//
// This test suite is designed to mimick the functional-tests.sh suite.
// We want to slowly re-write the bash test to golang.
//
// FULL LIST OF BASH TESTS from functional-tests.sh
//
//  DONE test_make_bucket
//	DONE test_make_bucket_error
//  DONE test_rb
//
//  DONE test_put_object
//  DONE test_put_object_error
//  DONE test_put_object_0byte
//  DONE test_put_object_with_storage_class
//  DONE test_put_object_with_storage_class_error
//  DONE test_put_object_with_metadata
//  DONE test_put_object_multipart
//  DONE test_get_object
//  DONE test_get_object_multipart
//  DONE test_find
//  DONE  test_find_empty
//  DONE  test_od_object
//  DONE  test_mv_object
//    test_presigned_post_policy_error
//    test_presigned_put_object
//    test_presigned_get_object
//    test_cat_object
//    test_cat_stdin
//    test_copy_directory
//    test_mirror_list_objects
//    test_mirror_list_objects_storage_class
//    test_copy_object_preserve_filesystem_attr
//		test_watch_object
//
//	DONE test_put_object_with_sse
//	DONE	test_put_object_with_encoded_sse
//	DONE	test_put_object_with_sse_error
//  DONE  test_put_object_multipart_sse
//	DONE	test_get_object_with_sse
//	DONE	test_cat_object_with_sse
//	DONE	test_cat_object_with_sse_error
//	DONE	test_copy_object_with_sse_rewrite
//	DONE	test_copy_object_with_sse_dest
//	DONE	test_sse_key_rotation
//	DONE	test_mirror_with_sse
//	DONE	test_rm_object_with_sse
//
//    test_config_host_add
//    test_config_host_add_error
//    test_admin_users
//
//    teardown

var (
	OneMBSlice        [1048576]byte // 1x Mebibyte
	ALIAS             = "play"
	FileMap           = make(map[string]*testFile)
	RandomLargeString = "lksdjfljsdklfjklsdjfklksjdf;lsjdk;fjks;djflsdlfkjskldjfklkljsdfljsldkfjklsjdfkljsdklfjklsdjflksjdlfjsdjflsjdflsldfjlsjdflksjdflkjslkdjflksfdj"
	JSON              = "--json"
	JSONOutput        = true
	CMD               = "../mc"
	MetaPrefix        = "X-Amz-Meta-"

	ServerEndpoint           = "play.min.io"
	AcessKey                 = "Q3AM3UQ867SPQQA43P2F"
	SecretKey                = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	SkipBuild                = false
	Protocol                 = "http://"
	SkipInsecure             = false
	TempDir                  = ""
	MainTestBucket           string
	SSETestBucket            string
	BucketList               = make([]string, 0)
	BaseSSEKey               = "32byteslongsecretkeymustbegiven1"
	BaseSSEKey2              = "32byteslongsecretkeymustbegiven2"
	BaseInvalidSSEEncodedKey = "MzJieXRlc2xvbmdzZWNyZWFiY2RlZmcJZ2l2ZW5uM="
	BaseSSEEncodedKey        = "MzJieXRlc2xvbmdzZWNyZWFiY2RlZmcJZ2l2ZW5uMjE="
	BaseInvalidSSEKey        = "32byteslongsecretkeymustbe"
)

func GetMBSizeInBytes(MB int) int64 {
	return int64(MB * len(OneMBSlice))
}

func initializeTestSuite(t *testing.T) {
	shouldSkipBuild := os.Getenv("SKIP_BUILD")
	shouldSkipBool, _ := strconv.ParseBool(shouldSkipBuild)
	if !shouldSkipBool {
		err := BuildCLI()
		if err != nil {
			os.Exit(1)
		}
	}

	var err error
	TempDir, err = os.MkdirTemp("", "minio-mc-test")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	envALIAS := os.Getenv("ALIAS")
	if envALIAS != "" {
		ALIAS = envALIAS
	}
	envSecretKey := os.Getenv("SECRET_KEY")
	if envSecretKey != "" {
		SecretKey = envSecretKey
	}
	envAccessKey := os.Getenv("ACCESS_KEY")
	if envAccessKey != "" {
		AcessKey = envAccessKey
	}
	envServerEndpoint := os.Getenv("SERVER_ENDPOINT")
	if envServerEndpoint != "" {
		ServerEndpoint = envServerEndpoint
	}

	envSkipInsecure := os.Getenv("SKIP_INSECURE")
	if envSkipInsecure == "true" {
		SkipInsecure = true
	}

	envEnableHTTP := os.Getenv("ENABLE_HTTPS")
	if envEnableHTTP == "true" {
		Protocol = "https://"
	}

	envCMD := os.Getenv("CMD")
	if envCMD != "" {
		CMD = envCMD
	}

	for i := 0; i < len(OneMBSlice); i++ {
		OneMBSlice[i] = byte(rand.Intn(250))
	}

	createFile(newTestFile{
		tag:                "0M",
		prefix:             "",
		extension:          ".jpg",
		storageClass:       "",
		sizeInMBS:          0,
		tags:               map[string]string{"name": "0M"},
		uploadShouldFail:   false,
		addToGlobalFileMap: true,
	})
	createFile(newTestFile{
		tag:                "1M",
		prefix:             "",
		extension:          ".txt",
		storageClass:       "REDUCED_REDUNDANCY",
		sizeInMBS:          1,
		metaData:           map[string]string{"name": "1M"},
		tags:               map[string]string{"tag1": "1M-tag"},
		uploadShouldFail:   false,
		addToGlobalFileMap: true,
	})
	createFile(newTestFile{
		tag:                "2M",
		prefix:             "LVL1",
		extension:          ".jpg",
		storageClass:       "REDUCED_REDUNDANCY",
		sizeInMBS:          2,
		metaData:           map[string]string{"name": "2M"},
		uploadShouldFail:   false,
		addToGlobalFileMap: true,
	})
	createFile(newTestFile{
		tag:                "3M",
		prefix:             "LVL1/LVL2",
		extension:          ".png",
		storageClass:       "",
		sizeInMBS:          3,
		metaData:           map[string]string{"name": "3M"},
		uploadShouldFail:   false,
		addToGlobalFileMap: true,
	})
	createFile(newTestFile{
		tag:                "65M",
		prefix:             "LVL1/LVL2/LVL3",
		extension:          ".exe",
		storageClass:       "",
		sizeInMBS:          65,
		metaData:           map[string]string{"name": "65M", "tag1": "value1"},
		uploadShouldFail:   false,
		addToGlobalFileMap: true,
	})

	// ERROR FILES
	// This file is used to trigger error cases
	createFile(newTestFile{
		tag:                "E1",
		storageClass:       "UNKNOWN",
		extension:          ".png",
		sizeInMBS:          0,
		metaData:           map[string]string{},
		tags:               map[string]string{},
		uploadShouldFail:   true,
		addToGlobalFileMap: true,
	})

	cmd := make([]string, 0)
	cmd = append(cmd, "alias")
	cmd = append(cmd, "set")
	cmd = append(cmd, ALIAS)
	cmd = append(cmd, Protocol+ServerEndpoint)
	cmd = append(cmd, AcessKey)
	cmd = append(cmd, SecretKey)
	if SkipInsecure {
		cmd = append(cmd, "--insecure")
	}

	out, err := RunCommand(cmd...)
	if err != nil {
		log.Println(out)
		panic(err)
	}

	MainTestBucket = CreateBucket(t)
	SSETestBucket = CreateBucket(t)
}

func Test_FullSuite(t *testing.T) {
	defer func() {
		r := recover()
		if r != nil {
			log.Println(r, string(debug.Stack()))
		}

		CLEANUP(t)
	}()
	// initializeTestSuite builds the mc client and creates local files which are used for testing
	initializeTestSuite(t)
	// uploadAllFiles uploads all files in FileMap to MainTestBucket
	uploadAllFiles(t)
	// LSObjects saves the output of LS inside *testFile in FileMap
	LSObjects(t)
	// StatObjecsts saves the output of Stat inside *testFile in FileMap
	StatObjecsts(t)
	// ValidateFileMetaDataPostUpload validates the output of LS and Stat
	ValidateFileMetaData(t)

	// The following tests rely on files uploaded via (uploadAllFiles)

	// OD(t)
	FindObjects(t)
	FindObjectsUsingName(t)
	FindObjectsUsingNameAndFilteringForTxtType(t)
	FindObjectsLargerThan64Mebibytes(t)
	FindObjectsSmallerThan64Mebibytes(t)
	FindObjectsOlderThan1d(t)
	FindObjectsNewerThen1d(t)
	GetObjectsAndCompareMD5(t)
	CreateBucketUsingInvalidSymbols(t)
	RemoveBucketWithNameTooLong(t)
	RemoveBucketThatDoesNotExist(t)

	// SEE tests use the SSETestBucket
	PutObjectWithSSE(t)
	PutObjectWithSSEMultipart(t)
	PutObjectWithEncodedSSE(t)
	PutObjectWithSSEInvalidKeys(t)
	GetObjectWithSSE(t)
	GetObjectWithSSEWithoutKey(t)
	CatObjectWithSSE(t)
	CatObjectWithSSEWithoutKey(t)
	CopyObjectWithSSEToNewBucketWithNewKey(t)
	MirrorTempDirectoryUsingSSE(t)
	RemoveObjectWithSSE(t)

	// Independent tests
	MoveFileFromDiskToMinio(t)
}

func CreateBucket(t *testing.T) (bucketPath string) {
	bucketName := "mc-gh-actions-test-" + uuid.NewString()
	bucketPath = ALIAS + "/" + bucketName
	out, err := RunCommand("mb", bucketPath)
	if err != nil {
		t.Fatalf("Unable to create bucket (%s) err: %s", bucketPath, out)
		return
	}
	BucketList = append(BucketList, bucketPath)
	out, err = RunCommand("ls", ALIAS)
	if err != nil {
		t.Fatalf("Unable to ls alias (%s) err: %s", ALIAS, out)
		return
	}
	if !strings.Contains(out, bucketName) {
		t.Fatalf("LS output does not contain bucket name (%s)", bucketName)
	}
	return
}

func PutObjectWithSSEMultipart(t *testing.T) {
	file := FileMap["65M"]
	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, file.diskFile.Name())
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)

	out, err := RunCommand(p...)
	if err != nil {
		t.Fatal(err, out)
	}
}

func PutObjectWithSSE(t *testing.T) {
	file := FileMap["1M"]
	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, file.diskFile.Name())
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)
	out, err := RunCommand(p...)
	if err != nil {
		t.Fatal(err, out)
	}
}

func PutObjectWithEncodedSSE(t *testing.T) {
	file := FileMap["2M"]
	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEEncodedKey)
	p = append(p, file.diskFile.Name())
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath+".encodedkey")
	out, err := RunCommand(p...)
	if err != nil {
		t.Fatal(err, out)
	}
}

func PutObjectWithSSEInvalidKeys(t *testing.T) {
	file := FileMap["1M"]
	p1 := make([]string, 0)
	p1 = append(p1, "cp")
	p1 = append(p1, "--encrypt-key="+SSETestBucket+"="+BaseInvalidSSEKey)
	p1 = append(p1, file.diskFile.Name())
	p1 = append(p1, SSETestBucket+"/"+file.fileNameWithoutPath)
	out, err := RunCommand(p1...)
	if err == nil {
		t.Fatal(err, out)
	}

	p2 := make([]string, 0)
	p2 = append(p2, "cp")
	p2 = append(p2, "--encrypt-key="+SSETestBucket+"="+BaseInvalidSSEEncodedKey)
	p2 = append(p2, file.diskFile.Name())
	p2 = append(p2, SSETestBucket+"/"+file.fileNameWithoutPath)
	out, err = RunCommand(p2...)
	if err == nil {
		t.Fatal(err, out)
	}
}

func GetObjectWithSSE(t *testing.T) {
	file := FileMap["1M"]
	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)
	p = append(p, file.diskFile.Name()+".download")

	out, err := RunCommand(p...)
	fatalIfErrorWMsg(err, out, t)
}

func GetObjectWithSSEWithoutKey(t *testing.T) {
	file := FileMap["1M"]
	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)
	p = append(p, file.diskFile.Name()+"-get")

	out, err := RunCommand(p...)
	if err == nil {
		t.Fatal(err, out)
	}
}

func CatObjectWithSSE(t *testing.T) {
	file := FileMap["1M"]
	p := make([]string, 0)
	p = append(p, "cat")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)
	// p = append(p, file.diskFile.Name()+".cat")

	out, err := RunCommand(p...)
	fatalIfError(err, t)
	catMD5Sum := GetMD5Sum([]byte(out))

	if catMD5Sum != file.md5Sum {
		fatalMsgOnly(fmt.Sprintf(
			"expected md5sum %s but we got %s",
			file.md5Sum,
			catMD5Sum,
		), t)
	}

	if int64(len(out)) != file.MinioStat.Size {
		fatalMsgOnly(fmt.Sprintf(
			"file size is %d but we got %d",
			file.MinioStat.Size,
			len(out),
		), t)
	}

	fatalIfErrorWMsg(
		err,
		"cat length: "+strconv.Itoa(len(out))+" -- file length:"+strconv.Itoa(int(file.diskStat.Size())),
		t,
	)
}

func CatObjectWithSSEWithoutKey(t *testing.T) {
	file := FileMap["1M"]
	p := make([]string, 0)
	p = append(p, "cat")
	p = append(p, SSETestBucket+"/"+file.fileNameWithoutPath)
	p = append(p, file.diskFile.Name()+"-cat")

	out, err := RunCommand(p...)
	fatalIfNoErrorWMsg(err, out, t)
}

func RemoveObjectWithSSE(t *testing.T) {
	RemoveBucket := CreateBucket(t)

	file := createFile(newTestFile{
		tag:                "1M-RM",
		prefix:             "LVL1",
		extension:          ".png",
		storageClass:       "",
		sizeInMBS:          1,
		metaData:           map[string]string{"name": "1M"},
		uploadShouldFail:   false,
		addToGlobalFileMap: false,
	})

	p := make([]string, 0)
	p = append(p, "cp")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, file.diskFile.Name())
	p = append(p, RemoveBucket+"/"+file.fileNameWithoutPath)
	out, err := RunCommand(p...)
	fatalIfErrorWMsg(err, out, t)

	p2 := make([]string, 0)
	p2 = append(p2, "rm")
	p2 = append(p2, RemoveBucket+"/"+file.fileNameWithoutPath)
	out, err = RunCommand(p2...)
	fatalIfErrorWMsg(err, out, t)

	p3 := make([]string, 0)
	p3 = append(p3, "stat")
	p3 = append(p3, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p3 = append(p3, RemoveBucket+"/"+file.fileNameWithoutPath)
	out, err = RunCommand(p3...)
	fatalIfNoErrorWMsg(err, out, t)
}

func MirrorTempDirectoryUsingSSE(t *testing.T) {
	MirrorBucket := CreateBucket(t)

	p := make([]string, 0)
	p = append(p, "mirror")
	p = append(p, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey)
	p = append(p, TempDir)
	p = append(p, MirrorBucket)

	out, err := RunCommand(p...)
	fatalIfErrorWMsg(err, out, t)

	out, err = RunCommand("ls", "-r", MirrorBucket)
	fatalIfError(err, t)

	fileList, err := parseLSJSONOutput(out)
	fatalIfError(err, t)

	for i, f := range FileMap {
		fileFound := false

		for _, o := range fileList {
			if o.Key == f.fileNameWithoutPath {
				FileMap[i].MinioLS = o
				fileFound = true
			}
		}

		if !fileFound {
			t.Fatalf("File was not uploaded: %s", f.fileNameWithPrefix)
		}
	}
}

func CopyObjectWithSSEToNewBucketWithNewKey(t *testing.T) {
	TargetSSEBucket := CreateBucket(t)
	file := FileMap["1M"]

	p2 := make([]string, 0)
	p2 = append(p2, "cp")
	p2 = append(p2, "--encrypt-key="+SSETestBucket+"="+BaseSSEKey+","+TargetSSEBucket+"="+BaseSSEKey2)
	p2 = append(p2, SSETestBucket+"/"+file.fileNameWithoutPath)
	p2 = append(p2, TargetSSEBucket+"/"+file.fileNameWithoutPath)

	out, err := RunCommand(p2...)
	if err != nil {
		t.Fatal(err, out)
	}
}

func uploadAllFiles(t *testing.T) {
	for _, v := range FileMap {
		parameters := make([]string, 0)
		parameters = append(parameters, "cp")

		if v.storageClass != "" {
			parameters = append(parameters, "--storage-class", v.storageClass)
		}

		if len(v.metaData) > 0 {
			parameters = append(parameters, "--attr")
			meta := ""
			for i, v := range v.metaData {
				meta += i + "=" + v + ";"
			}
			meta = strings.TrimSuffix(meta, ";")
			parameters = append(parameters, meta)
		}
		if len(v.tags) > 0 {
			parameters = append(parameters, "--tags")
			tags := ""
			for i, v := range v.tags {
				tags += i + "=" + v + ";"
			}
			tags = strings.TrimSuffix(tags, ";")
			parameters = append(parameters, tags)
		}

		parameters = append(parameters, v.diskFile.Name())

		if v.prefix != "" {
			parameters = append(
				parameters,
				MainTestBucket+"/"+v.fileNameWithPrefix,
			)
		} else {
			parameters = append(
				parameters,
				MainTestBucket+"/"+v.fileNameWithoutPath,
			)
		}

		_, err := RunCommand(parameters...)
		if err != nil {
			if !v.uploadShouldFail {
				t.Fatal(err)
			}
		}
	}
}

func OD(t *testing.T) {
	LocalBucketPath := CreateBucket(t)

	file := FileMap["65M"]
	out, err := RunCommand(
		"od",
		"if="+file.diskFile.Name(),
		"of="+LocalBucketPath+"/od/"+file.fileNameWithoutPath,
		"parts=10",
	)

	fatalIfError(err, t)
	odMsg, err := parseSingleODMessageJSONOutput(out)
	fatalIfError(err, t)

	if odMsg.TotalSize != file.diskStat.Size() {
		t.Fatalf(
			"Expected (%d) bytes to be uploaded but only uploaded (%d) bytes",
			odMsg.TotalSize,
			file.diskStat.Size(),
		)
	}

	if odMsg.Parts != 10 {
		t.Fatalf(
			"Expected upload parts to be (10) but they were (%d)",
			odMsg.Parts,
		)
	}

	if odMsg.Type != "FStoS3" {
		t.Fatalf(
			"Expected type to be (FStoS3) but got (%s)",
			odMsg.Type,
		)
	}

	if odMsg.PartSize != uint64(file.diskStat.Size())/10 {
		t.Fatalf(
			"Expected part size to be (%d) but got (%d)",
			file.diskStat.Size()/10,
			odMsg.PartSize,
		)
	}

	out, err = RunCommand(
		"od",
		"of="+file.diskFile.Name(),
		"if="+LocalBucketPath+"/od/"+file.fileNameWithoutPath,
		"parts=10",
	)

	fatalIfError(err, t)
	fmt.Println(out)
	odMsg, err = parseSingleODMessageJSONOutput(out)
	fatalIfError(err, t)

	if odMsg.TotalSize != file.diskStat.Size() {
		t.Fatalf(
			"Expected (%d) bytes to be uploaded but only uploaded (%d) bytes",
			odMsg.TotalSize,
			file.diskStat.Size(),
		)
	}

	if odMsg.Parts != 10 {
		t.Fatalf(
			"Expected upload parts to be (10) but they were (%d)",
			odMsg.Parts,
		)
	}

	if odMsg.Type != "S3toFS" {
		t.Fatalf(
			"Expected type to be (FStoS3) but got (%s)",
			odMsg.Type,
		)
	}

	if odMsg.PartSize != uint64(file.diskStat.Size())/10 {
		t.Fatalf(
			"Expected part size to be (%d) but got (%d)",
			file.diskStat.Size()/10,
			odMsg.PartSize,
		)
	}
}

func MoveFileFromDiskToMinio(t *testing.T) {
	LocalBucketPath := CreateBucket(t)

	file := createFile(newTestFile{
		tag:                "10Move",
		prefix:             "",
		extension:          ".txt",
		storageClass:       "",
		sizeInMBS:          1,
		metaData:           map[string]string{"name": "10Move"},
		tags:               map[string]string{"tag1": "10Move-tag"},
		uploadShouldFail:   false,
		addToGlobalFileMap: false,
	})

	out, err := RunCommand(
		"mv",
		file.diskFile.Name(),
		LocalBucketPath+"/"+file.fileNameWithoutPath,
	)

	fatalIfError(err, t)
	splitReturn := bytes.Split([]byte(out), []byte{10})

	mvMSG, err := parseSingleCPMessageJSONOutput(string(splitReturn[0]))
	fatalIfError(err, t)

	if mvMSG.TotalCount != 1 {
		t.Fatalf("Expected count to be 1 but got (%d)", mvMSG.TotalCount)
	}

	if mvMSG.Size != file.diskStat.Size() {
		t.Fatalf(
			"Expected size to be (%d) but got (%d)",
			file.diskStat.Size(),
			mvMSG.Size,
		)
	}

	if mvMSG.Status != "success" {
		t.Fatalf(
			"Expected status to be (success) but got (%s)",
			mvMSG.Status,
		)
	}

	statMSG, err := parseSingleAccountStatJSONOutput(string(splitReturn[1]))
	fatalIfError(err, t)

	if statMSG.Transferred != file.diskStat.Size() {
		t.Fatalf(
			"Expected transfeered to be (%d) but got (%d)",
			file.diskStat.Size(),
			statMSG.Transferred,
		)
	}

	if statMSG.Total != file.diskStat.Size() {
		t.Fatalf(
			"Expected total to be (%d) but got (%d)",
			file.diskStat.Size(),
			statMSG.Total,
		)
	}

	if statMSG.Status != "success" {
		t.Fatalf(
			"Expected status to be (success) but got (%s)",
			statMSG.Status,
		)
	}
}

func LSObjects(t *testing.T) {
	out, err := RunCommand("ls", "-r", MainTestBucket)
	fatalIfError(err, t)

	fileList, err := parseLSJSONOutput(out)
	fatalIfError(err, t)

	for i, f := range FileMap {
		fileFound := false

		for _, o := range fileList {
			if o.Key == f.fileNameWithPrefix {
				FileMap[i].MinioLS = o
				fileFound = true
			}
		}

		if !f.uploadShouldFail && !fileFound {
			t.Fatalf("File was not uploaded: %s", f.fileNameWithPrefix)
		} else if f.uploadShouldFail && fileFound {
			t.Fatalf("File should not have been uploaded: %s", f.fileNameWithPrefix)
		}
	}
}

func StatObjecsts(t *testing.T) {
	for i, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		out, err := RunCommand(
			"stat",
			MainTestBucket+"/"+v.fileNameWithPrefix,
		)
		fatalIfError(err, t)

		FileMap[i].MinioStat, err = parseStatSingleObjectJSONOutput(out)
		fatalIfError(err, t)

		if FileMap[i].MinioStat.Key == "" {
			t.Fatalf("Unable to stat Minio object (%s)", v.fileNameWithPrefix)
		}

	}
}

func ValidateFileMetaData(t *testing.T) {
	for _, f := range FileMap {
		if f.uploadShouldFail {
			continue
		}
		validateFileLSInfo(t, f)
		validateObjectMetaData(t, f)
		// validateContentType(t, f)
	}
}

func FindObjects(t *testing.T) {
	out, err := RunCommand("find", MainTestBucket)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.MinioLS.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.MinioLS.Key)
		}
	}
}

func FindObjectsUsingName(t *testing.T) {
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		out, err := RunCommand(
			"find",
			MainTestBucket,
			"--name",
			v.fileNameWithoutPath,
		)

		fatalIfError(err, t)
		info, err := parseFindSingleObjectJSONOutput(out)
		fatalIfError(err, t)
		if !strings.HasSuffix(info.Key, v.MinioLS.Key) {
			t.Fatalf("Invalid key (%s) when searching for (%s)", info.Key, v.MinioLS.Key)
		}

	}
}

func FindObjectsUsingNameAndFilteringForTxtType(t *testing.T) {
	out, err := RunCommand(
		"find",
		MainTestBucket,
		"--name",
		"*.txt",
	)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	for _, v := range FileMap {
		if v.uploadShouldFail || v.extension != ".txt" {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.MinioLS.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.MinioLS.Key)
		}
	}
}

func FindObjectsSmallerThan64Mebibytes(t *testing.T) {
	out, err := RunCommand(
		"find",
		MainTestBucket,
		"--smaller",
		"64MB",
	)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	for _, v := range FileMap {
		if v.uploadShouldFail || v.diskStat.Size() > GetMBSizeInBytes(64) {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.MinioLS.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.MinioLS.Key)
		}
	}
}

func FindObjectsLargerThan64Mebibytes(t *testing.T) {
	out, err := RunCommand(
		"find",
		MainTestBucket,
		"--larger",
		"64MB",
	)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	for _, v := range FileMap {
		if v.uploadShouldFail || v.diskStat.Size() < GetMBSizeInBytes(64) {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.MinioLS.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.MinioLS.Key)
		}
	}
}

func FindObjectsOlderThan1d(t *testing.T) {
	out, err := RunCommand(
		"find",
		MainTestBucket,
		"--older-than",
		"1d",
	)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	if len(findList) > 0 {
		t.Fatalf("We should not have found any files which are older then 1 day")
	}
}

func FindObjectsNewerThen1d(t *testing.T) {
	out, err := RunCommand(
		"find",
		MainTestBucket,
		"--newer-than",
		"1d",
	)
	fatalIfError(err, t)

	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)

	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.MinioLS.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.MinioLS.Key)
		}
	}
}

func GetObjectsAndCompareMD5(t *testing.T) {
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		// make sure old downloads are not in our way
		_ = os.Remove(TempDir + "/" + v.fileNameWithoutPath + ".downloaded")

		_, err := RunCommand(
			"cp",
			MainTestBucket+"/"+v.fileNameWithPrefix,
			TempDir+"/"+v.fileNameWithoutPath+".downloaded",
		)
		fatalIfError(err, t)

		downloadedFile, err := os.Open(
			TempDir + "/" + v.fileNameWithoutPath + ".downloaded",
		)
		fatalIfError(err, t)

		fileBytes, err := io.ReadAll(downloadedFile)
		md5sum := GetMD5Sum(fileBytes)

		if v.md5Sum != md5sum {
			t.Fatalf(
				"The downloaded file md5sum is wrong: original-md5(%s) downloaded-md5(%s)",
				v.md5Sum,
				md5sum,
			)
		}
	}
}

func CreateBucketUsingInvalidSymbols(t *testing.T) {
	bucketNameMap := make(map[string]string)
	bucketNameMap["name-too-big"] = RandomLargeString
	bucketNameMap["!"] = "symbol!"
	bucketNameMap["@"] = "symbol@"
	bucketNameMap["#"] = "symbol#"
	bucketNameMap["$"] = "symbol$"
	bucketNameMap["%"] = "symbol%"
	bucketNameMap["^"] = "symbol^"
	bucketNameMap["&"] = "symbol&"
	bucketNameMap["*"] = "symbol*"
	bucketNameMap["("] = "symbol("
	bucketNameMap[")"] = "symbol)"
	bucketNameMap["{"] = "symbol{"
	bucketNameMap["}"] = "symbol}"
	bucketNameMap["["] = "symbol["
	bucketNameMap["]"] = "symbol]"

	for _, v := range bucketNameMap {
		_, err := RunCommand("mb", ALIAS+"/"+v)
		if err == nil {
			t.Fatalf("We should not have been able to create a bucket with the name: %s", v)
		}
	}
}

func RemoveBucketThatDoesNotExist(t *testing.T) {
	randomID := uuid.NewString()
	out, _ := RunCommand(
		"rb",
		ALIAS+"/"+randomID,
	)
	errMSG, _ := parseSingleErrorMessageJSONOutput(out)
	validateErrorMSGValues(
		t,
		errMSG,
		"error",
		"Unable to validate",
		"does not exist",
	)
}

func RemoveBucketWithNameTooLong(t *testing.T) {
	randomID := uuid.NewString()
	out, _ := RunCommand(
		"rb",
		ALIAS+"/"+randomID+randomID,
	)
	errMSG, _ := parseSingleErrorMessageJSONOutput(out)
	validateErrorMSGValues(
		t,
		errMSG,
		"error",
		"Unable to validate",
		"Bucket name cannot be longer than 63 characters",
	)
}

func UploadToUnknownBucket(t *testing.T) {
	randomBucketID := uuid.NewString()
	parameters := append(
		[]string{},
		"cp",
		FileMap["1M"].diskFile.Name(),
		ALIAS+"/"+randomBucketID+"-test-should-not-exist"+"/"+FileMap["1M"].fileNameWithoutPath,
	)

	_, err := RunCommand(parameters...)
	if err == nil {
		t.Fatalf("We should not have been able to upload to bucket: %s", randomBucketID)
	}
}

func CLEANUP(t *testing.T) {
	var err error
	var berr error
	err = os.RemoveAll(TempDir)
	if err != nil {
		fmt.Println(err)
	}

	for _, v := range BucketList {
		var out string
		out, berr = RunCommand("rb", "--force", "--dangerous", v)
		if berr != nil {
			fmt.Printf("Unable to remove bucket bucket (%s) err: %s //  out: %s", v, err, out)
		}
	}

	if berr != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func validateFileLSInfo(t *testing.T, file *testFile) {
	if file.diskStat.Size() != int64(file.MinioLS.Size) {
		t.Fatalf(
			"File and minio object are not the same size - Object (%d) vs File (%d)",
			file.MinioLS.Size,
			file.diskStat.Size(),
		)
	}
	// if file.md5Sum != file.findOutput.Etag {
	// 	t.Fatalf("File and file.findOutput do not have the same md5Sum - Object (%s) vs File (%s)", file.findOutput.Etag, file.md5Sum)
	// }
	if file.storageClass != "" {
		if file.storageClass != file.MinioLS.StorageClass {
			t.Fatalf(
				"File and minio object do not have the same storage class - Object (%s) vs File (%s)",
				file.MinioLS.StorageClass,
				file.storageClass,
			)
		}
	} else {
		if file.MinioLS.StorageClass != "STANDARD" {
			t.Fatalf(
				"Minio object was expected to have storage class (STANDARD) but it was (%s)",
				file.MinioLS.StorageClass,
			)
		}
	}
}

func validateObjectMetaData(t *testing.T, file *testFile) {
	for i, v := range file.metaData {
		found := false

		for ii, vv := range file.MinioStat.Metadata {
			if MetaPrefix+strings.Title(i) == ii {
				found = true
				if v != vv {
					fmt.Println("------------------------")
					fmt.Println("META CHECK")
					fmt.Println(file.MinioStat.Metadata)
					fmt.Println(file.metaData)
					fmt.Println("------------------------")
					t.Fatalf("Meta values are not the same v1(%s) v2(%s)", v, vv)
				}
			}
		}

		if !found {
			fmt.Println("------------------------")
			fmt.Println("META CHECK")
			fmt.Println(file.MinioStat.Metadata)
			fmt.Println(file.metaData)
			fmt.Println("------------------------")
			t.Fatalf("Meta tag(%s) not found", i)
		}

	}
}

// func validateContentType(t *testing.T, file *testFile) {
// 	value, ok := file.MinioStat.Metadata["Content-Type"]
// 	if !ok {
// 		t.Fatalf("File (%s) did not have a content type", file.fileNameWithPrefix)
// 		return
// 	}
//
// 	contentType := mime.TypeByExtension(file.extension)
// 	if contentType != value {
// 		log.Println(file)
// 		log.Println(file.MinioLS)
// 		log.Println(file.extension)
// 		log.Println(file.MinioStat)
// 		t.Fatalf("Content types on file (%s) do not match, extension(%s) File(%s) MinIO object(%s)", file.fileNameWithPrefix, file.extension, contentType, file.MinioStat.Metadata["Content-Type"])
// 	}
// }

func GetSource(skip int) (out string) {
	pc := make([]uintptr, 3) // at least 1 entry needed
	runtime.Callers(skip, pc)
	f := runtime.FuncForPC(pc[0])
	file, line := f.FileLine(pc[0])
	sn := strings.Split(f.Name(), ".")
	var name string
	if sn[len(sn)-1] == "func1" {
		name = sn[len(sn)-2]
	} else {
		name = sn[len(sn)-1]
	}
	out = file + ":" + fmt.Sprint(line) + ":" + name
	return
}

func GetMD5Sum(data []byte) string {
	md5Writer := md5.New()
	md5Writer.Write(data)
	return fmt.Sprintf("%x", md5Writer.Sum(nil))
}

func fatalMsgOnly(msg string, t *testing.T) {
	fmt.Println(GetSource(3))
	t.Fatal(msg)
}

func fatalIfNoErrorWMsg(err error, msg string, t *testing.T) {
	if err == nil {
		fmt.Println(GetSource(3))
		fmt.Println(msg)
		t.Fatal(err)
	}
}

func fatalIfNoError(err error, t *testing.T) {
	if err == nil {
		fmt.Println(GetSource(3))
		t.Fatal(err)
	}
}

func fatalIfErrorWMsg(err error, msg string, t *testing.T) {
	if err != nil {
		fmt.Println(GetSource(3))
		fmt.Println(msg)
		t.Fatal(err)
	}
}

func fatalIfError(err error, t *testing.T) {
	if err != nil {
		fmt.Println(GetSource(3))
		t.Fatal(err)
	}
}

func parseFindJSONOutput(out string) (findList []*findMessage, err error) {
	findList = make([]*findMessage, 0)
	splitList := bytes.Split([]byte(out), []byte{10})

	for _, v := range splitList {
		if len(v) < 1 {
			continue
		}
		line := new(findMessage)
		err = json.Unmarshal(v, line)
		if err != nil {
			return
		}
		findList = append(findList, line)
	}

	fmt.Println("FIND LIST ------------------------------")
	for _, v := range findList {
		fmt.Println(v)
	}
	fmt.Println(" ------------------------------")
	return
}

func parseLSJSONOutput(out string) (lsList []contentMessage, err error) {
	lsList = make([]contentMessage, 0)
	splitList := bytes.Split([]byte(out), []byte{10})

	for _, v := range splitList {
		if len(v) < 1 {
			continue
		}
		line := contentMessage{}
		err = json.Unmarshal(v, &line)
		if err != nil {
			return
		}
		lsList = append(lsList, line)
	}

	fmt.Println("LS LIST ------------------------------")
	for _, v := range lsList {
		fmt.Println(v)
	}
	fmt.Println(" ------------------------------")
	return
}

func parseFindSingleObjectJSONOutput(out string) (findInfo contentMessage, err error) {
	err = json.Unmarshal([]byte(out), &findInfo)
	if err != nil {
		return
	}

	fmt.Println("FIND SINGLE OBJECT ------------------------------")
	fmt.Println(findInfo)
	fmt.Println(" ------------------------------")
	return
}

func parseStatSingleObjectJSONOutput(out string) (stat statMessage, err error) {
	err = json.Unmarshal([]byte(out), &stat)
	if err != nil {
		return
	}

	fmt.Println("STAT ------------------------------")
	fmt.Println(stat)
	fmt.Println(" ------------------------------")
	return
}

// We have to wrap the error output because the console
// printing mechanism for json marshals into an anonymous
// object before printing
// see cmd/error.go line 70
type errorMessageWrapper struct {
	Error  errorMessage `json:"error"`
	Status string       `json:"status"`
}

func validateErrorMSGValues(
	t *testing.T,
	errMSG errorMessageWrapper,
	TypeToValidate string,
	MessageToValidate string,
	CauseToValidate string,
) {
	if TypeToValidate != "" {
		if !strings.Contains(errMSG.Error.Type, TypeToValidate) {
			t.Fatalf(
				"Expected error.Error.Type to contain (%s) - but got (%s)",
				TypeToValidate,
				errMSG.Error.Type,
			)
		}
	}
	if MessageToValidate != "" {
		if !strings.Contains(errMSG.Error.Message, MessageToValidate) {
			t.Fatalf(
				"Expected error.Error.Message to contain (%s) - but got (%s)",
				MessageToValidate,
				errMSG.Error.Message,
			)
		}
	}
	if CauseToValidate != "" {
		if !strings.Contains(errMSG.Error.Cause.Message, CauseToValidate) {
			t.Fatalf(
				"Expected error.Error.Cause.Message to contain (%s) - but got (%s)",
				CauseToValidate,
				errMSG.Error.Cause.Message,
			)
		}
	}
}

func parseSingleErrorMessageJSONOutput(out string) (errMSG errorMessageWrapper, err error) {
	err = json.Unmarshal([]byte(out), &errMSG)
	if err != nil {
		return
	}

	fmt.Println("ERROR ------------------------------")
	fmt.Println(errMSG)
	fmt.Println(" ------------------------------")
	return
}

func parseSingleODMessageJSONOutput(out string) (odMSG odMessage, err error) {
	err = json.Unmarshal([]byte(out), &odMSG)
	if err != nil {
		return
	}

	return
}

func parseSingleAccountStatJSONOutput(out string) (stat accountStat, err error) {
	err = json.Unmarshal([]byte(out), &stat)
	if err != nil {
		return
	}

	return
}

func parseSingleCPMessageJSONOutput(out string) (cpMSG copyMessage, err error) {
	err = json.Unmarshal([]byte(out), &cpMSG)
	if err != nil {
		return
	}

	return
}

type newTestFile struct {
	tag              string // The tag used to identify the file inside the FileMap. This tag is also used in the objects name.
	prefix           string // Prefix for the object name ( not including the object name itself)
	extension        string
	storageClass     string
	sizeInMBS        int
	uploadShouldFail bool // Set this to true if this file is used for detecting errors and should not be found after the upload phase
	metaData         map[string]string
	tags             map[string]string

	addToGlobalFileMap bool
}

type testFile struct {
	newTestFile
	MinioLS             contentMessage
	MinioStat           statMessage
	diskFile            *os.File
	diskStat            os.FileInfo
	md5Sum              string
	fileNameWithoutPath string
	fileNameWithPrefix  string
}

func (f *testFile) String() (out string) {
	out = fmt.Sprintf(
		"Size: %d || Name: %s || md5Sum: %s",
		f.diskStat.Size(),
		f.fileNameWithoutPath,
		f.md5Sum,
	)
	return
}

func createFile(nf newTestFile) (newTestFile *testFile) {
	newFile, err := os.CreateTemp(TempDir, nf.tag+"-mc-test-file-*"+nf.extension)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	md5Writer := md5.New()
	for i := 0; i < nf.sizeInMBS; i++ {
		n, err := newFile.Write(OneMBSlice[:])
		mn, merr := md5Writer.Write(OneMBSlice[:])
		if err != nil || merr != nil {
			log.Println(err)
			log.Println(merr)
			return nil
		}
		if n != len(OneMBSlice) {
			log.Println("Did not write 1MB to file")
			return nil
		}
		if mn != len(OneMBSlice) {
			log.Println("Did not write 1MB to md5sum writer")
			return nil
		}
	}
	splitName := strings.Split(newFile.Name(), string(os.PathSeparator))
	fileNameWithoutPath := splitName[len(splitName)-1]
	md5sum := fmt.Sprintf("%x", md5Writer.Sum(nil))
	stats, err := newFile.Stat()
	if err != nil {
		return nil
	}
	newTestFile = &testFile{
		md5Sum:              md5sum,
		fileNameWithoutPath: fileNameWithoutPath,
		diskFile:            newFile,
		diskStat:            stats,
	}

	newTestFile.tag = nf.tag
	newTestFile.metaData = nf.metaData
	newTestFile.storageClass = nf.storageClass
	newTestFile.sizeInMBS = nf.sizeInMBS
	newTestFile.uploadShouldFail = nf.uploadShouldFail
	newTestFile.tags = nf.tags
	newTestFile.prefix = nf.prefix
	newTestFile.extension = nf.extension

	if nf.prefix != "" {
		newTestFile.fileNameWithPrefix = nf.prefix + "/" + fileNameWithoutPath
	} else {
		newTestFile.fileNameWithPrefix = fileNameWithoutPath
	}
	if nf.addToGlobalFileMap {
		FileMap[nf.tag] = newTestFile
	}
	return newTestFile
}

func BuildCLI() error {
	out, err := exec.Command("go", "build", "../.").CombinedOutput()
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println(out)
	return nil
}

func RunCommand(parameters ...string) (out string, err error) {
	log.Println("EXEC |>", CMD, strings.Join(parameters, " "))
	var outBytes []byte
	var outErr error

	if JSONOutput {
		parameters = append([]string{JSON}, parameters...)
	}

	if SkipInsecure {
		parameters = append(parameters, []string{"--insecure"}...)
	}

	outBytes, outErr = exec.Command(CMD, parameters...).CombinedOutput()
	out = string(outBytes)
	err = outErr
	return
}
