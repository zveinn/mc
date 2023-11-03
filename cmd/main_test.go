package cmd

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// This test suite is designed to mimick the functional-tests.sh suite.
// We want to slowly re-write the bash test to golang.
//
// Test Suite Flow:
// 1. Set aliases
// 2. Create base bucket for test suite
// 3. Upload 0MB file
// 4. Upload 1MB file
// 5. Upload 65MB file - this triggers multipart
// 6. Upload Broken file
// 7. Validate file sizes + md5sums (TODO: md5sums need work)
// 8. Validate file metadata
// 9. validate file tags
// 10. test creating invalid buckets
// 11. test upload to unknown buckets
//
//
//
//
// FULL LIST OF BASH TESTS
//
//  DONE test_make_bucket
//	DONE test_make_bucket_error (needs to cover all failure cases)
//  DONE test_rb
//
//  ???? test_list_dir (... we list alot ????)
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
//    test_od_object
//    test_mv_object
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
//		test_put_object_with_sse
//		test_put_object_with_encoded_sse
//		test_put_object_with_sse_error
//		test_put_object_multipart_sse
//		test_get_object_with_sse
//		test_cat_object_with_sse
//		test_cat_object_with_sse_error
//		test_copy_object_with_sse_rewrite
//		test_copy_object_with_sse_dest
//		test_sse_key_rotation
//		test_mirror_with_sse
//		test_rm_object_with_sse
//
//    test_config_host_add
//    test_config_host_add_error
//    test_admin_users
//
//    teardown

var (
	OneMBSlice [1048576]byte
	BUCKET_ID  = "mc-gh-actions-test-" + uuid.NewString()
	ALIAS      = "play"
	FileMap    = make(map[string]*testFile)
)

func GetMBSizeInBytes(MB int) int64 {
	return int64(MB * len(OneMBSlice))
}

const (
	RANDOM_LARGE_STRING = "lksdjfljsdklfjklsdjfklksjdf;lsjdk;fjks;djflsdlfkjskldjfklkljsdfljsldkfjklsjdfkljsdklfjklsdjflksjdlfjsdjflsjdflsldfjlsjdflksjdflkjslkdjflksfdj"
	JSON                = "--json"
	JSON_OutPut         = true
	CMD                 = "../mc"
	META_PREFIX         = "X-Amz-Meta-"

	SERVER_ENDPOINT = "play.min.io"
	ACCESS_KEY      = "Q3AM3UQ867SPQQA43P2F"
	SECRET_KEY      = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
)

func init() {
	err := BuildCLI()
	if err != nil {
		os.Exit(1)
	}

	for i := 0; i < len(OneMBSlice); i++ {
		OneMBSlice[i] = byte(rand.Intn(250))
	}

	// CreateFile("0M", 0, "REDUCED_REDUNDANCY")
	CreateFile(newTestFile{
		tag:              "0M",
		prefix:           "",
		extension:        ".jpg",
		storageClass:     "",
		sizeInMBS:        0,
		tags:             map[string]string{"name": "0M"},
		uploadShouldFail: false,
	})
	CreateFile(newTestFile{
		tag:              "1M",
		prefix:           "",
		extension:        ".txt",
		storageClass:     "REDUCED_REDUNDANCY",
		sizeInMBS:        1,
		metaData:         map[string]string{"name": "1M"},
		tags:             map[string]string{"tag1": "1M-tag"},
		uploadShouldFail: false,
	})
	CreateFile(newTestFile{
		tag:              "2M",
		prefix:           "LVL1",
		extension:        ".jpg",
		storageClass:     "REDUCED_REDUNDANCY",
		sizeInMBS:        2,
		metaData:         map[string]string{"name": "2M"},
		uploadShouldFail: false,
	})
	CreateFile(newTestFile{
		tag:              "3M",
		prefix:           "LVL1/LVL2",
		extension:        ".png",
		storageClass:     "",
		sizeInMBS:        3,
		metaData:         map[string]string{"name": "3M"},
		uploadShouldFail: false,
	})
	CreateFile(newTestFile{
		tag:              "65M",
		prefix:           "LVL1/LVL2/LVL3",
		extension:        ".exe",
		storageClass:     "",
		sizeInMBS:        65,
		metaData:         map[string]string{"name": "65M", "tag1": "value1"},
		uploadShouldFail: false,
	})

	// ERROR FILES
	// This file is used to trigger error cases
	CreateFile(newTestFile{
		tag:              "E1",
		storageClass:     "UNKNOWN",
		extension:        ".png",
		sizeInMBS:        0,
		metaData:         map[string]string{},
		tags:             map[string]string{},
		uploadShouldFail: true,
	})

	out, err := RunCommand(
		"alias",
		"set",
		ALIAS,
		"https://"+SERVER_ENDPOINT,
		ACCESS_KEY,
		SECRET_KEY,
	)
	if err != nil {
		log.Println(out)
		panic(err)
	}
}

func CreateFile(nf newTestFile) {
	newFile, err := os.CreateTemp("", nf.tag+"-mc-test-file-*"+nf.extension)
	if err != nil {
		log.Println(err)
		return
	}
	md5Writer := md5.New()
	for i := 0; i < nf.sizeInMBS; i++ {
		n, err := newFile.Write(OneMBSlice[:])
		mn, merr := md5Writer.Write(OneMBSlice[:])
		if err != nil || merr != nil {
			log.Println(err)
			log.Println(merr)
			return
		}
		if n != len(OneMBSlice) {
			log.Println("Did not write 1MB to file")
			return
		}
		if mn != len(OneMBSlice) {
			log.Println("Did not write 1MB to md5sum writer")
			return
		}
	}
	splitName := strings.Split(newFile.Name(), string(os.PathSeparator))
	fileNameWithoutPath := splitName[len(splitName)-1]
	md5sum := fmt.Sprintf("%x", md5Writer.Sum(nil))
	stats, err := newFile.Stat()
	if err != nil {
		return
	}
	FileMap[nf.tag] = &testFile{
		md5Sum:              md5sum,
		fileNameWithoutPath: fileNameWithoutPath,
		file:                newFile,
		stat:                stats,
	}
	FileMap[nf.tag].tag = nf.tag
	FileMap[nf.tag].metaData = nf.metaData
	FileMap[nf.tag].storageClass = nf.storageClass
	FileMap[nf.tag].sizeInMBS = nf.sizeInMBS
	FileMap[nf.tag].uploadShouldFail = nf.uploadShouldFail
	FileMap[nf.tag].tags = nf.tags
	FileMap[nf.tag].prefix = nf.prefix
	FileMap[nf.tag].extension = nf.extension
	if nf.prefix != "" {
		FileMap[nf.tag].fileNameWithPrefix = nf.prefix + "/" + fileNameWithoutPath
	} else {
		FileMap[nf.tag].fileNameWithPrefix = fileNameWithoutPath
	}
	return
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

	if JSON_OutPut {
		parameters = append([]string{JSON}, parameters...)
		outBytes, outErr = exec.Command(CMD, parameters...).CombinedOutput()
	} else {
		outBytes, outErr = exec.Command(CMD, parameters...).CombinedOutput()
	}

	out = string(outBytes)
	err = outErr
	return
}

func Test_MakeBucketForTestSuite(t *testing.T) {
	_, err := RunCommand("mb", ALIAS+"/"+BUCKET_ID)
	fatalIfError(err, t)
	out, err := RunCommand("ls", ALIAS)
	fatalIfError(err, t)
	if !strings.Contains(out, BUCKET_ID) {
		t.Fatalf("Expected bucket (" + BUCKET_ID + ") to exist on the endpoint, but it did not.")
	}
}

func Test_MakeAndRemoveBucketWithError(t *testing.T) {
	// TODO: find all failure cases...
}

func Test_UploadAllFiles(t *testing.T) {
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

		parameters = append(parameters, v.file.Name())

		if v.prefix != "" {
			parameters = append(
				parameters,
				ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithPrefix,
			)
		} else {
			parameters = append(
				parameters,
				ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithoutPath,
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

func Test_LSObjectsAndSaveResults(t *testing.T) {
	out, err := RunCommand("ls", "-r", ALIAS+"/"+BUCKET_ID)
	fatalIfError(err, t)

	fileList, err := parseLSJSONOutput(out)
	fatalIfError(err, t)

	for i, f := range FileMap {
		fileFound := false

		for _, o := range fileList {
			if o.Key == f.fileNameWithPrefix {
				FileMap[i].LSOutput = o
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

func Test_StatObjecstsAndSaveResults(t *testing.T) {
	for i, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}
		out, err := RunCommand("stat", ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithPrefix)
		fatalIfError(err, t)
		FileMap[i].MinIOStat, err = parseStatSingleObjectJSONOutput(out)
		if FileMap[i].MinIOStat.Key == "" {
			t.Fatalf("Unable to stat Minio object (%s)", v.fileNameWithPrefix)
		}
		fatalIfError(err, t)
	}
}

func Test_ValidateFileMetaDataPostUpload(t *testing.T) {
	for _, f := range FileMap {
		if f.uploadShouldFail {
			continue
		}
		validateFileLSInfo(t, f)
		validateObjectMetaData(t, f)
		// validateContentType(t, f)
	}
}

func Test_FindObjects(t *testing.T) {
	out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID)
	fatalIfError(err, t)
	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.LSOutput.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.LSOutput.Key)
		}
	}
}

func Test_FindObjectsFullName(t *testing.T) {
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}
		out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID, "--name", v.fileNameWithoutPath)
		fatalIfError(err, t)
		info, err := parseFindSingleObjectJSONOutput(out)
		fatalIfError(err, t)
		if !strings.HasSuffix(info.Key, v.LSOutput.Key) {
			t.Fatalf("Invalid key (%s) when searching for (%s)", info.Key, v.LSOutput.Key)
		}
	}
}

func Test_FindObjectsNameFilterTxtFile(t *testing.T) {
	out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID, "--name", "*.txt")
	fatalIfError(err, t)
	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)
	for _, v := range FileMap {
		if v.uploadShouldFail || v.extension != ".txt" {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.LSOutput.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.LSOutput.Key)
		}
	}
}

func Test_FindObjectsLargerThan(t *testing.T) {
	out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID, "--larger", "64MB")
	fatalIfError(err, t)
	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)
	for _, v := range FileMap {
		log.Println("SIZES: ", v.stat.Size(), GetMBSizeInBytes(64))
		if v.uploadShouldFail || v.stat.Size() < GetMBSizeInBytes(64) {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.LSOutput.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.LSOutput.Key)
		}
	}
}

func Test_FindObjectsOlderThan1d(t *testing.T) {
	out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID, "--older-than", "1d")
	fatalIfError(err, t)
	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)
	if len(findList) > 0 {
		t.Fatalf("We should not have found any files which are older then 1 day")
	}
}

func Test_FindObjectsNewerThen1d(t *testing.T) {
	out, err := RunCommand("find", ALIAS+"/"+BUCKET_ID, "--newer-than", "1d")
	fatalIfError(err, t)
	findList, err := parseFindJSONOutput(out)
	fatalIfError(err, t)
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}

		found := false
		for _, vv := range findList {
			if strings.HasSuffix(vv.Key, v.LSOutput.Key) {
				found = true
			}
		}

		if !found {
			t.Fatalf("File (%s) not found by 'find' command", v.LSOutput.Key)
		}
	}
}

func Test_GetObjects(t *testing.T) {
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}
		// make sure old downloads are not in our way
		_ = os.Remove(os.TempDir() + "/" + v.fileNameWithoutPath + ".downloaded")

		_, err := RunCommand("cp", ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithPrefix, os.TempDir()+"/"+v.fileNameWithoutPath+".downloaded")
		fatalIfError(err, t)

		downloadedFile, err := os.Open(os.TempDir() + "/" + v.fileNameWithoutPath + ".downloaded")
		fatalIfError(err, t)
		md5Writer := md5.New()
		fileBytes, err := io.ReadAll(downloadedFile)
		fatalIfError(err, t)
		md5Writer.Write(fileBytes)
		md5sum := fmt.Sprintf("%x", md5Writer.Sum(nil))
		if v.md5Sum != md5sum {
			t.Fatalf("The downloaded file md5sum is wrong: original-md5(%s) downloaded-md5(%s)", v.md5Sum, md5sum)
		}
	}
}

func fatalIfError(err error, t *testing.T) {
	if err != nil {
		t.Fatal(err)
	}
}

func Test_CreateBucketFailure(t *testing.T) {
	bucketNameMap := make(map[string]string)
	bucketNameMap["name-too-big"] = RANDOM_LARGE_STRING
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

func Test_UploadToUnknownBucket(t *testing.T) {
	randomBucketID := uuid.NewString()
	parameters := append([]string{}, "cp", FileMap["1M"].file.Name(), ALIAS+"/"+randomBucketID+"-test-should-not-exist"+"/"+FileMap["1M"].fileNameWithoutPath)

	_, err := RunCommand(parameters...)
	if err == nil {
		t.Fatalf("We should not have been able to upload to bucket: %s", randomBucketID)
	}
}

func Test_RemoveBucketUsedForTestSuite(t *testing.T) {
	_, err := RunCommand("rb", ALIAS+"/"+BUCKET_ID, "--force")
	fatalIfError(err, t)
	out, err := RunCommand("ls", ALIAS)
	fatalIfError(err, t)
	if strings.Contains(out, BUCKET_ID) {
		t.Fatalf("Expected bucket (" + BUCKET_ID + ") to NOT exist on the endpoint, but it did not.")
	}
}

func Test_RemoveFilesPostTesting(_ *testing.T) {
	for _, v := range FileMap {
		_ = os.Remove(v.file.Name())
		if !v.uploadShouldFail {
			_ = os.Remove(os.TempDir() + "/" + v.fileNameWithoutPath + ".downloaded")
		}
	}
}

func validateFileLSInfo(t *testing.T, file *testFile) {
	if file.stat.Size() != int64(file.LSOutput.Size) {
		t.Fatalf("File and minio object are not the same size - Object (%d) vs File (%d)", file.LSOutput.Size, file.stat.Size())
	}
	// if file.md5Sum != file.findOutput.Etag {
	// 	t.Fatalf("File and file.findOutput do not have the same md5Sum - Object (%s) vs File (%s)", file.findOutput.Etag, file.md5Sum)
	// }
	if file.storageClass != "" {
		if file.storageClass != file.LSOutput.StorageClass {
			t.Fatalf("File and minio object do not have the same storage class - Object (%s) vs File (%s)", file.LSOutput.StorageClass, file.storageClass)
		}
	} else {
		if file.LSOutput.StorageClass != "STANDARD" {
			t.Fatalf("Minio object was expected to have storage class (STANDARD) but it was (%s)", file.LSOutput.StorageClass)
		}
	}
}

func validateObjectMetaData(t *testing.T, file *testFile) {
	for i, v := range file.metaData {
		found := false

		for ii, vv := range file.MinIOStat.Metadata {
			if META_PREFIX+strings.Title(i) == ii {
				found = true
				if v != vv {
					fmt.Println("------------------------")
					fmt.Println("META CHECK")
					fmt.Println(file.MinIOStat.Metadata)
					fmt.Println(file.metaData)
					fmt.Println("------------------------")
					t.Fatalf("Meta values are not the same v1(%s) v2(%s)", v, vv)
				}
			}
		}

		if !found {
			fmt.Println("------------------------")
			fmt.Println("META CHECK")
			fmt.Println(file.MinIOStat.Metadata)
			fmt.Println(file.metaData)
			fmt.Println("------------------------")
			t.Fatalf("Meta tag(%s) not found", i)
		}

	}
}

func validateContentType(t *testing.T, file *testFile) {
	value, ok := file.MinIOStat.Metadata["Content-Type"]
	if !ok {
		t.Fatalf("File (%s) did not have a content type", file.fileNameWithPrefix)
		return
	}

	contentType := mime.TypeByExtension(file.extension)
	if contentType != value {
		log.Println(file)
		log.Println(file.LSOutput)
		log.Println(file.extension)
		log.Println(file.MinIOStat)
		t.Fatalf("Content types on file (%s) do not match, extension(%s) File(%s) MinIO object(%s)", file.fileNameWithPrefix, file.extension, contentType, file.MinIOStat.Metadata["Content-Type"])
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

type newTestFile struct {
	tag              string
	prefix           string
	extension        string
	metaData         map[string]string
	storageClass     string
	sizeInMBS        int
	uploadShouldFail bool
	tags             map[string]string
}
type testFile struct {
	newTestFile
	LSOutput            contentMessage
	MinIOStat           statMessage
	file                *os.File
	stat                os.FileInfo
	md5Sum              string
	fileNameWithoutPath string
	fileNameWithPrefix  string
}

func (f *testFile) String() (out string) {
	out = fmt.Sprintf("Size: %d || Name: %s || md5Sum: %s", f.stat.Size(), f.fileNameWithoutPath, f.md5Sum)
	return
}
