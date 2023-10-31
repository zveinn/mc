package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

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

// FULL LIST OF BASH TESTS
//
//  DONE  test_make_bucket
//	DONE	test_make_bucket_error (needs to cover all failure cases)
//  DONE  test_rb
//
//  ???? test_list_dir (... we list alot ????)
//  DONE  test_put_object
//  DONE  test_put_object_error
//  DONE  test_put_object_0byte
//  DONE test_put_object_with_storage_class
//  DONE  test_put_object_with_storage_class_error
//  DONE  test_put_object_with_metadata
//  DONE test_put_object_multipart
//    test_get_object
//    test_get_object_multipart
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
//    test_find
//    test_find_empty
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
	FileMap    = make(map[string]*TestFile)
)

const (
	RANDOM_LARGE_STRING = "lksdjfljsdklfjklsdjfklksjdf;lsjdk;fjks;djflsdlfkjskldjfklkljsdfljsldkfjklsjdfkljsdklfjklsdjflksjdlfjsdjflsjdflsldfjlsjdflksjdflkjslkdjflksfdj"
	JSON                = "--json"
	JSON_OutPut         = true
	CMD                 = "./mc"
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
	CreateFile(NewTestFile{
		tag:              "0M",
		storageClass:     "",
		sizeInMBS:        0,
		tags:             map[string]string{"name": "0M"},
		uploadShouldFail: false,
	})
	CreateFile(NewTestFile{
		tag:              "1M",
		storageClass:     "REDUCED_REDUNDANCY",
		sizeInMBS:        1,
		metaData:         map[string]string{"name": "1M"},
		tags:             map[string]string{"tag1": "1M-tag"},
		uploadShouldFail: false,
	})
	CreateFile(NewTestFile{
		tag:              "65M",
		storageClass:     "",
		sizeInMBS:        65,
		metaData:         map[string]string{"name": "65M", "tag1": "value1"},
		uploadShouldFail: false,
	})

	// ERROR FILES
	CreateFile(NewTestFile{
		tag:              "E1",
		storageClass:     "UNKNOWN",
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

func CreateFile(nf NewTestFile) {
	newFile, err := os.CreateTemp("", nf.tag+"-mc-test-file-*")
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
	FileMap[nf.tag] = &TestFile{
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
	return
}

func BuildCLI() error {
	out, err := exec.Command("go", "build", ".").CombinedOutput()
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println(out)
	return nil
}

func RunCommand(parameters ...string) (out string, err error) {
	log.Println("EXEC$", CMD, strings.Join(parameters, " "))
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
	if err != nil {
		t.Fatal(err)
	}
	out, err := RunCommand("ls", ALIAS)
	if err != nil {
		t.Fatal(err)
	}
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

		parameters = append(parameters, v.file.Name(), ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithoutPath)

		_, err := RunCommand(parameters...)
		if err != nil {
			if !v.uploadShouldFail {
				t.Fatal(err)
			}
		}
	}

	out, err := RunCommand("ls", ALIAS+"/"+BUCKET_ID)
	if err != nil {
		t.Fatal(err)
	}

	fileList, err := parseLSJSONOutput(out)
	if err != nil {
		t.Fatal(err)
	}

	for i, f := range FileMap {
		fileFound := false
		for _, o := range fileList {
			if f.fileNameWithoutPath == o.Key {
				FileMap[i].LSObject = *o
				fileFound = true
				compareLocalFileToRemoteObjectInfo(t, f, o)
			}
		}

		if !f.uploadShouldFail && !fileFound {
			t.Fatalf("File was not uploaded: %s", f.fileNameWithoutPath)
		} else if f.uploadShouldFail && fileFound {
			t.Fatalf("File should not have been uploaded: %s", f.fileNameWithoutPath)
		}
	}
}

func Test_StatObjecst(t *testing.T) {
	for _, v := range FileMap {
		if v.uploadShouldFail {
			continue
		}
		out, err := RunCommand("stat", ALIAS+"/"+BUCKET_ID+"/"+v.fileNameWithoutPath)
		if err != nil {
			t.Fatal(err)
		}
		object, err := parseStatSingleObject(out)
		if err != nil {
			t.Fatal(err)
		}
		validateObjectMetaData(t, v, object)

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
	if err != nil {
		t.Fatal(err)
	}
	out, err := RunCommand("ls", ALIAS)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, BUCKET_ID) {
		t.Fatalf("Expected bucket (" + BUCKET_ID + ") to NOT exist on the endpoint, but it did not.")
	}
}

func compareLocalFileToRemoteObjectInfo(t *testing.T, file *TestFile, object *LSObject) {
	if file.stat.Size() != int64(object.Size) {
		fmt.Println()
		fmt.Println("------------------------")
		fmt.Println("COMPARING FILE TO OBJECT")
		fmt.Println(object)
		fmt.Println(file)
		fmt.Println("------------------------")
		t.Fatalf("File and object are not the same size - Object (%d) vs File (%d)", object.Size, file.stat.Size())
	}
	// if file.md5Sum != object.Etag {
	// 	t.Fatalf("File and object do not have the same md5Sum - Object (%s) vs File (%s)", object.Etag, file.md5Sum)
	// }
	if file.storageClass != "" {
		if file.storageClass != object.StorageClass {
			fmt.Println()
			fmt.Println("------------------------")
			fmt.Println("COMPARING FILE TO OBJECT")
			fmt.Println(object)
			fmt.Println(file)
			fmt.Println("------------------------")
			t.Fatalf("File and object do not have the same storageClass - Object (%s) vs File (%s)", object.StorageClass, file.storageClass)
		}
	}
}

func validateObjectMetaData(t *testing.T, file *TestFile, object *StatObject) {
	for i, v := range file.metaData {
		found := false

		for ii, vv := range object.MetaData {
			if META_PREFIX+strings.Title(i) == ii {
				found = true
				if v != vv {
					fmt.Println("------------------------")
					fmt.Println("META CHECK")
					fmt.Println(object)
					fmt.Println(file)
					fmt.Println("------------------------")
					t.Fatalf("Meta values are not the same v1(%s) v2(%s)", v, vv)
				}
			}
		}

		if !found {
			fmt.Println("------------------------")
			fmt.Println("META CHECK")
			fmt.Println(object)
			fmt.Println(file)
			fmt.Println("------------------------")
			t.Fatalf("Meta tag(%s) not found", i)
		}

	}
}

func parseStatSingleObject(out string) (object *StatObject, err error) {
	object = new(StatObject)
	err = json.Unmarshal([]byte(out), object)
	if err != nil {
		return
	}
	return
}

func parseLSJSONOutput(out string) (lsList []*LSObject, err error) {
	lsList = make([]*LSObject, 0)
	splitList := bytes.Split([]byte(out), []byte{10})

	for _, v := range splitList {
		if len(v) < 1 {
			continue
		}
		line := new(LSObject)
		err = json.Unmarshal(v, line)
		if err != nil {
			return
		}
		lsList = append(lsList, line)
	}

	fmt.Println("")
	fmt.Println("LS LIST ------------------------------")
	for _, v := range lsList {
		fmt.Println(v)
	}
	fmt.Println(" ------------------------------")
	fmt.Println("")
	return
}

type LSObject struct {
	Status         string            `json:"status"`
	Type           string            `json:"type"`
	LastModified   time.Time         `json:"lastModified"`
	Size           int               `json:"size"`
	Key            string            `json:"key"`
	Etag           string            `json:"etag"`
	URL            string            `json:"url"`
	VersionOrdinal int               `json:"versionOrdinal"`
	StorageClass   string            `json:"storageClass"`
	MetaData       map[string]string `json:"metadata"`
	Tags           map[string]string `json:"tags"`
}

func (o *LSObject) String() (out string) {
	out = fmt.Sprintf("Size: %d || Key: %s || Etag: %s || Type: %s || Status: %s || Mod: %s || VersionOrdinal: %d || StorageClass: %s", o.Size, o.Key, o.Etag, o.Type, o.Status, o.LastModified, o.VersionOrdinal, o.StorageClass)
	return
}

type StatObject struct {
	Status         string            `json:"status"`
	Type           string            `json:"type"`
	LastModified   time.Time         `json:"lastModified"`
	Size           int               `json:"size"`
	Name           string            `json:"name"`
	Etag           string            `json:"etag"`
	URL            string            `json:"url"`
	VersionOrdinal int               `json:"versionOrdinal"`
	StorageClass   string            `json:"storageClass"`
	MetaData       map[string]string `json:"metadata"`
	Tags           map[string]string `json:"tags"`
}

func (o *StatObject) String() (out string) {
	out = fmt.Sprintf("Size: %d || Name: %s || Etag: %s || Type: %s || Status: %s || Mod: %s || VersionOrdinal: %d || StorageClass: %s", o.Size, o.Name, o.Etag, o.Type, o.Status, o.LastModified, o.VersionOrdinal, o.StorageClass)
	return
}

type NewTestFile struct {
	tag              string
	metaData         map[string]string
	storageClass     string
	sizeInMBS        int
	uploadShouldFail bool
	tags             map[string]string
}
type TestFile struct {
	NewTestFile
	LSObject            LSObject
	file                *os.File
	stat                os.FileInfo
	md5Sum              string
	fileNameWithoutPath string
}

func (f *TestFile) String() (out string) {
	out = fmt.Sprintf("Size: %d || Name: %s || md5Sum: %s", f.stat.Size(), f.fileNameWithoutPath, f.md5Sum)
	return
}
