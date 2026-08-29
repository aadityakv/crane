package file_sys

import (
	"fmt"
	"io"
	"io/ioutil"
	// "log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"shared"
)

// TODO: Currently doesn't support files with slashes in name
// According to stack overflow, we can map '/' to the unicode division '/'
// https://github.com/ncw/rclone/issues/62

const SDFS_Folder = "sdfs_files/"
const versionDelimeter = "~"
const maxNumVersions = 4

var fileSysLog = shared.OpenLogFile(fmt.Sprintf("fileSys%d.log", shared.GetOwnServerNumber()))

func PutFile(localFname, sdfsFname string, localFile []byte) error {
	curVersions, globErr := filepath.Glob(sdfsFname + versionDelimeter + "*")
	if globErr != nil {
		return fmt.Errorf("there is an error: %s: %v\n", globErr.Error(), sdfsFname)
	}

	// Move the versions up by one for each file
	for i := len(curVersions) - 1; i >= 0; i-- {
		file := curVersions[i]
		splitStr := strings.Split(file, versionDelimeter)
		if vers, notIntErr := strconv.Atoi(splitStr[1]); notIntErr != nil || vers > maxNumVersions {
			os.Remove(file)
		}
		os.Rename(file, fmt.Sprintf("%s%d", splitStr[0]+versionDelimeter, i+2))
	}

	// Append versionDelimeter1 to the end of the input file
	os.Rename(sdfsFname, sdfsFname+versionDelimeter+"1")

	sdfsF, sdfsErr := os.OpenFile(sdfsFname, os.O_RDWR|os.O_CREATE, 0600)
	if sdfsErr != nil {
		return fmt.Errorf("File error on sdfs file: %s\n", sdfsErr)
	}
	_, writeErr := sdfsF.Write(localFile)
	if writeErr != nil {
		return writeErr
	}
	fileSysLog.Printf("Wrote contents of %s to %s", localFname, sdfsFname)
	return nil
}

func GetFile(sdfsFname, localFname string) (s []byte, e error) {
	var empty []byte
	s = empty

	if _, err := os.Stat(sdfsFname); os.IsNotExist(err) {
		e = fmt.Errorf("File %s does not exist\n", sdfsFname)
		return
	}

	s, e = ioutil.ReadFile(sdfsFname)
	return
}

func DeleteFile(sdfsFname string) (bool, error) {
	onMachine := false
	if _, err := os.Stat(sdfsFname); !os.IsNotExist(err) {
		onMachine = true
	}
	os.Remove(sdfsFname)
	versions, globErr := filepath.Glob(sdfsFname + versionDelimeter + "*")
	if globErr != nil {
		return onMachine, fmt.Errorf("%s: %v\n", globErr.Error(), sdfsFname)
	}
	for _, file := range versions {
		os.Remove(file)
	}
	fileSysLog.Printf("Deleted %s and its %d versions", sdfsFname, len(versions))
	return onMachine, nil
}

func LSFile(sdfsFname string) (bool, error) {
	onMachine := false
	if _, err := os.Stat(sdfsFname); !os.IsNotExist(err) {
		onMachine = true
	}
	return onMachine, nil
}

func Store() error {
	files, err := ioutil.ReadDir(SDFS_Folder)
	if err != nil {
		return fmt.Errorf("Store error: %v\n", err)
	}
	for _, f := range files {
		fmt.Printf("%v %10d  %s\n", f.ModTime().Format("2006-01-02 15:04:05.000"), f.Size(), f.Name())
	}
	return nil
}

func GetVersions(sdfsFname string, numVersions int, localFname string) (s []byte, e error) {
	var empty []byte

	versions, globErr := filepath.Glob(sdfsFname + versionDelimeter + "*")
	versions = append([]string{sdfsFname}, versions...)
	if globErr != nil {
		return empty, fmt.Errorf("%s: %v\n", globErr.Error(), sdfsFname)
	}

	// Delete/clear local file if it already exists
	tempFname := SDFS_Folder + "temptemptemp"
	os.Remove(tempFname)

	tempF, destErr := os.OpenFile(tempFname, os.O_RDWR|os.O_CREATE, 0600)
	if destErr != nil {
		return empty, fmt.Errorf("Error opening dest file: %s\n", destErr)
	}
	defer tempF.Close()

	// Write versions to the output file
	for i, file := range versions {
		if i+1 > numVersions {
			continue
		}

		tempF.WriteString(fmt.Sprintf("\n\n----------- Version %d -----------\n", i))

		copyErr := copyFile(file, tempF)
		if copyErr != nil {
			os.Remove(tempFname)
			return empty, copyErr
		}
	}

	// Return the contents of the temporary file
	s, e = ioutil.ReadFile(tempFname)
	os.Remove(tempFname)
	return
}

func ReceiveFile(baseFname string, contents []byte) error {
	sdfsFname := baseFname
	os.Remove(sdfsFname)

	// Possible race conditions but I think it doesn't matter because each one does the same thing
	sdfsF, sdfsErr := os.OpenFile(sdfsFname, os.O_RDWR|os.O_CREATE, 0600)
	if sdfsErr != nil {
		return fmt.Errorf("File error with replication of: %s\n", sdfsErr)
	}
	_, writeErr := sdfsF.Write(contents)
	if writeErr != nil {
		return writeErr
	}
	fileSysLog.Printf("Updated contents of %s", sdfsFname)
	return nil
}

func copyFile(src string, destF *os.File) error {
	srcF, srcErr := os.Open(src)
	if srcErr != nil {
		return fmt.Errorf("Error opening src file: %s\n", srcErr)
	}
	defer srcF.Close()

	buf := make([]byte, 1024)
	for {
		n, readErr := srcF.Read(buf)
		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("Error reading src file: %s\n", readErr)
		}
		if n == 0 {
			break
		}
		if _, writeErr := destF.Write(buf[:n]); writeErr != nil {
			return fmt.Errorf("Error writing dest file: %s\n", writeErr)
		}
	}
	return nil
}

func Initialize() {
	// TODO: Uncomment when not testing stuff
	os.RemoveAll(SDFS_Folder)
	os.Mkdir(SDFS_Folder, os.ModePerm)
	go ListenForMembershipListChanges()
	go openFilePortForRPCInGoRoutine()
}

func Leave() {

}

func ReplaceSlashWithDivision(filename string) string {
	return strings.Replace(filename, "/", "∕", -1)
}

func HandleFileCmd(cmd string, args []string) error {
	switch cmd {
	case "put":
		{
			if len(args) != 2 {
				return fmt.Errorf("usage: %s local_filename sdfs_filename", cmd)
			}
			if strings.Contains(args[0], "~") {
				return fmt.Errorf("Local filename cannot contain %s character\n", versionDelimeter)
			}

			content, err := ioutil.ReadFile(args[0])
			if err != nil {
				return err
			}

			putArgs := shared.FileArgs{args[0], SDFS_Folder + ReplaceSlashWithDivision(args[1]), content, 0}
			putErr := MakeRemoteCall("Put", putArgs)
			return putErr
		}
	case "get":
		{
			if len(args) != 2 {
				return fmt.Errorf("usage: %s sdfs_filename local_filename", cmd)
			}
			if strings.Contains(args[1], "~") {
				return fmt.Errorf("Local filename cannot contain %s character\n", versionDelimeter)
			}
			var emptyByteArray []byte
			getArgs := shared.FileArgs{args[1], SDFS_Folder + ReplaceSlashWithDivision(args[0]), emptyByteArray, 0}
			return MakeRemoteCall("Get", getArgs)
		}
	case "delete":
		{
			if len(args) != 1 {
				return fmt.Errorf("usage: %s sdfs_filename", cmd)
			}
			var emptyByteArray []byte
			deleteArgs := shared.FileArgs{"", SDFS_Folder + ReplaceSlashWithDivision(args[0]), emptyByteArray, 0}
			deleteErr := MakeRemoteCall("Delete", deleteArgs)
			return deleteErr
		}
	case "ls":
		{
			if len(args) != 1 {
				return fmt.Errorf("usage: %s sdfs_filename", cmd)
			}
			var emptyByteArray []byte
			lsArgs := shared.FileArgs{"", SDFS_Folder + ReplaceSlashWithDivision(args[0]), emptyByteArray, 0}
			lsErr := MakeRemoteCall("LS", lsArgs)
			return lsErr
		}
	case "store":
		{
			if len(args) != 0 {
				return fmt.Errorf("usage: %s", cmd)
			}
			return Store()
		}
	case "get-versions":
		{
			if len(args) != 3 {
				return fmt.Errorf("usage: %s sdfs_filename numversions localfilename", cmd)
			}
			if strings.Contains(args[2], "~") {
				return fmt.Errorf("Local filename cannot contain %s character\n", versionDelimeter)
			}
			numVersions, _ := strconv.Atoi(args[1])
			var emptyByteArray []byte
			getVerArgs := shared.FileArgs{args[2], SDFS_Folder + ReplaceSlashWithDivision(args[0]), emptyByteArray, numVersions}
			return MakeRemoteCall("GetVersions", getVerArgs)
		}
	case "test":
		{
			if len(args) != 1 {
				return fmt.Errorf("usage: %s sdfs_filename", cmd)
			}
			old := []bool{false, true, false, true, true, true, true, true, true, true, true}
			new := []bool{false, true, true, true, true, true, true, true, true, true, true}

			//replicas := GetMachinesHoldingFileFromMemList(SDFS_Folder+ReplaceSlashWithDivision(args[0]), old)
			//fmt.Printf("replicas: %v\n", replicas)

			return SendReplicas(old, new)
		}
		/*case "test": {
		  if len(args) != 3 {
		    return fmt.Errorf("usage: %s sdfs_filename contents server", cmd)
		  }
		  serverNum, _ := strconv.Atoi(args[2])
		  bytes := []byte(args[1])
		  return RemoteSendFile(SDFS_Folder + args[0], bytes, serverNum)
		}*/
	}
	return nil
}

func ListenForMembershipListChanges() error {
	for {
		memListStruct := <-shared.GlobalMembershipChannel

		SendReplicas(memListStruct.OldList, memListStruct.NewList)
	}

	return nil
}

func SendReplicas(oldMemList, newMemList []bool) error {
	// Get a list of files
	files, fileErr := ioutil.ReadDir(SDFS_Folder)
	if fileErr != nil {
		return fileErr
	}

	// Check if each file should be sent
	for _, f := range files {
		fmt.Printf("Checking if %s needs to be sent\n", f.Name())
		// Ignore the version when hashing the file
		splitName := strings.Split(f.Name(), versionDelimeter)
		oldServers := GetMachinesHoldingFileFromMemList(SDFS_Folder+splitName[0], oldMemList)
		newServers := GetMachinesHoldingFileFromMemList(SDFS_Folder+splitName[0], newMemList)

		for i := 1; i <= 10; i++ {
			// Send file if new server should now have it
			if oldServers[i] == false && newServers[i] == true {
				contents, err := ioutil.ReadFile(SDFS_Folder + f.Name())
				if err != nil {
					return err
				}

				RemoteSendFile(SDFS_Folder+f.Name(), contents, i)
			}
		}

		// Delete file from local sdfs if the server should no longer have it
		ownServerNum := shared.GetOwnServerNumber()
		if oldServers[ownServerNum] == true && newServers[ownServerNum] == false {
			os.Remove(SDFS_Folder + f.Name())
		}
	}

	return nil
}
