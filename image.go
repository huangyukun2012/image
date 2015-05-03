package  main 

import(
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"strconv"
	"time"
	"net/http"
	"wharf/util"
)
const (
	BLOCKSIZE=1024*1024//1M
	HTTP_HEAD_SIZE=1024*512
)

/*=====================Imageconfig======================*/

type ImageConfig struct{
	Port string
}

func (c *ImageConfig)Init()error{
	filename := "/etc/wharf/image.conf"
	reader , err := os.Open(filename)	
	if err != nil{
		util.PrintErr(filename, err)	
		return err
	}
	err = util.UnmarshalReader(reader, c)	

	return err 
}

var imageConfig ImageConfig

/*===ImageTransportHead===*/
type ImageTransportHead struct{
	Net string	`192.168`
	Filename string	
	DataIndex int
	BlockNum int
	Nodes []string `1.1`
	Server	string `ip`
}

func (i *ImageTransportHead)GetDataFromHttpReqest( r *http.Request) error{
	content := make([]byte, BLOCKSIZE)
	n, err := r.Body.Read(content)	

	if err == io.EOF{
		if *flagDebug{
			util.PrintErr("Transport Head:")
			util.PrintErr(string(content[:n]))
		}
	}else if err != nil{
		util.PrintErr("Can not read ImageTransportHead from http request.")
		return err	
	}else{//err == nil
		newbuf := make([]byte, 200)
		_, testend := r.Body.Read(newbuf)
		if testend != io.EOF{
			util.PrintErr("we do not read all the content in the r.Body.")
			util.PrintErr("testenderr is", testend, string(newbuf[:10]))
			return errors.New("read uncompleted for ImageTransportHead!") 
		}
	}
	content = content[:n]	
	jsonerr:= json.Unmarshal(content, i)
	return jsonerr
}

/*===TransportUnit===*/
type TransportUnit struct{
	Meta	ImageTransportHead
	Body	[]byte
}

func (t *TransportUnit)Init( input ImageTransportHead){
	t.Meta=input
	return 
}

func (i *TransportUnit)GetDataFromHttpReqest( r *http.Request) error{

	content := make([]byte, r.ContentLength+1)

	var addition  int
	var readlen int
	addition, err := r.Body.Read(content)	
	defer r.Body.Close()
	readlen += addition

	for ;err!=io.EOF;{
		if err != nil{
			util.PrintErr("Can not read ImageTransportUnit from http request.")
			return err	
		}else{
			addition, err = r.Body.Read(content[readlen:]) 
			readlen += addition
		}
	}
	
	content = content[:readlen]	
	jsonerr:= json.Unmarshal(content, i)
	return jsonerr
}

/*===ImageTransportResponse===*/
type ImageTransportResponse struct{
	Status string
	ErrInfo string
}

func (itr *ImageTransportResponse)Set(status, err string){
	itr.Status = status
	itr.ErrInfo = err
}

func (itr *ImageTransportResponse)String()string{
	data, _:= json.Marshal(*itr)
	return string(data)
}


/*
function: transport the "fileName" to the Ip sets.
	cut the file into different blocks(sizeof 512K).
	send each block.
	if len(nodes)<1
			return OK,"no destination"
	loop:each block
		data_index++
			send block to nodes[0]
				success : do nothing,go to loop
				fail: return with errinfo
	end

	After the sent is over, wait for timeout(:the last block to the last ip_index. during the timeout):
		get the information from the last node, and return success 

		no info from the last node, return err


Input: fileName and Ip set 
output: 
	the last node send ack to the first node---- success: status 200-OK
											---- fail: status  Server error, imageFailNodes{ ip, errinfo}
timeout: ( size/bind + blockSize/bind * nodeNum ) *2  ---fail: status 408-timeout
Be carefull: if the file is one block size, we will read two times!!!
*/
func TransportImageHandler( w http.ResponseWriter, r *http.Request){
	defer r.Body.Close()
	clockClosed=false
	if *flagDebug {
		util.PrintErr("[ TransportImageHandler ]")
	}
	var response ImageTransportResponse
	var imt ImageTransportHead
	err := imt.GetDataFromHttpReqest( r )
	if err!=nil{
		http.Error(w, "bad request",400)
		response.Set(util.SERVER_ERROR, err.Error())	
		io.WriteString(w, response.String())
		return 
	}

	if len(imt.Nodes)<1{
		response.Set(util.OK,"no destination.")
		io.WriteString(w, response.String())
		return 
	}

	path :=`save_post` 
	destination := imt.Net + "." + imt.Nodes[0]			
	imt.Nodes = imt.Nodes[1:]
	endpoint := destination+":"+imageConfig.Port//note  that the server node can not be used as the resoucrce node
	url := `http://` + endpoint + `/` + path
	if *flagDebug {
		util.PrintErr("Post data to ", url)
	}

	//This where we decide to put the file
	f, openerr := os.Open(`/tmp/`+imt.Filename)	
	defer f.Close()
	if openerr != nil{
		response.Set(util.SERVER_ERROR, "We can not found the image in the server.")	
		return 
	}

	/* fileReader := bufio.NewReader(f) */
	thisFileInfo, staterr := f.Stat()
	if staterr!=nil{
		http.Error(w, "server error",500)	
		return 
	}
	//be carefull: if the file size is 10M, then it will have 10 block, but it will send 11 times.
	fileSizeByBlock := thisFileInfo.Size()/BLOCKSIZE
	if thisFileInfo.Size()%BLOCKSIZE>0{
		fileSizeByBlock++
	}
	imt.BlockNum=int(fileSizeByBlock)

	head := "reading file:(data_index/fileSizeByBlock)"
	if *flagDebug{
		fmt.Fprintf(os.Stdout, "%s", head)
	}
	imt.DataIndex=0

	for ; imt.DataIndex < imt.BlockNum && Process==true;{//every block
		buf := make([]byte,BLOCKSIZE)
		n , _ := f.Read(buf)
		if *flagDebug{
			util.Progress(int64(imt.DataIndex),fileSizeByBlock)
		}
		var postData TransportUnit
		postData.Init(imt)
		postData.Body = buf[:n]

		postBytes, jsonerr := json.Marshal(postData)
		if jsonerr != nil{
			util.PrintErr("In TransportImageHandler:we can not marshal the post data")	
			response.Set(util.SERVER_ERROR, jsonerr.Error())
			return 
		}
		if imt.DataIndex>=2{
			nextnode := destination
			endpoint := nextnode + `:` + imageConfig.Port 
			path := `/get_postState`
			url :=  `http://`+endpoint+path

			indexstr := strconv.Itoa(imt.DataIndex-2)
			resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(indexstr))
			contents := make([]byte, 10)
			if err == nil && strings.HasPrefix(resp.Status, "200") {
				defer resp.Body.Close()
				n , _:= resp.Body.Read(contents)	
				if string(contents[:n])=="true"{
					//continue
					if *flagDebug && DebugLevel>=2{
						util.PrintErr("the next node has posted the", indexstr, "data")	
					}
				}
			}else{
				if *flagDebug && DebugLevel>=2{
					util.PrintErr("the next node failed to post the", indexstr, "data")	
				}
				break//do not send any more
			}
		}

		res , reserr := http.Post(url, util.POSTTYPE, strings.NewReader(string(postBytes)))
		if reserr!= nil{//post failed
			util.PrintErr(reserr.Error())	
			response.Set(util.SERVER_ERROR, reserr.Error())
			return 
		}
		if strings.HasPrefix(res.Status, "200"){//post OK
			//do nothing
			res.Body.Close()
		}else{//post failed
			util.PrintErr(res.Status)	
			response.Set(util.SERVER_ERROR, res.Status)
			return 
		}
		imt.DataIndex++
	}

	if Process==false{
		http.Error(w,"server err",500 )	
		response.Set(util.SERVER_ERROR, "The send process is not complished")
		io.WriteString(w, response.String())
		return 
	}

	//wait for the answer from the last node
	bindwidth := 10
	duration := time.Duration( (1.0*imt.BlockNum/bindwidth+ (len(imt.Nodes)+1)/bindwidth )*5)*time.Second	
	util.PrintErr( "ALL blocks is sent. Waiting ack for", duration, "second ...")	
	isTimeOut := util.Timer(duration,&clock)
	if isTimeOut{
		response.Set(util.SERVER_ERROR, "time out.the transportation may be fail.")	
		util.PrintErr("Error: Time out for transport image.")
	}else{
		response.Set(util.OK, "transportation is ok.")	
		if *flagDebug{
			util.PrintErr("Transportation for the file is ok.")
		}
	}
	io.WriteString(w, response.String())				
	return 
}

/*
function:
	Save the block and post the block to next node.

	if *data_index* == 0: create a file with the given filename, and store it.	
	else just open the file, and add it. 

	** nodes = nodes[1:]
	if len(nodes)>=1:
		send the data to node[0]:
			success:do nothing
			fail: return errinfo, and send "fail to server"
	else://the last node
		if  data_index != -1://the last block 
			do nothing
		else
			send ac to server.
*/
func SaveAndPostHandler( w http.ResponseWriter, r *http.Request){
	var imt TransportUnit 
	err := imt.GetDataFromHttpReqest( r )

	if err!=nil{
		util.PrintErr(err)
		http.Error(w,"bad request", 400)
		sendAckToServer(false,imt.Meta.Server )
		return 
	}
	index := imt.Meta.DataIndex
	chs_save[index]=make(chan bool)
	chs_post[index]=make(chan bool)


	if imt.Meta.DataIndex == 0{
		go  saveBlock(imt,  chs_save[index])
		go  postBlock(imt,  chs_post[index])
	}else{
		go func(){//save
			value_save := <- chs_save[index-1]//wait
			if value_save==true {
				saveBlock(imt,  chs_save[index])	
			}else{
				util.PrintErr("Save", index-1, "failed.")
				sendAckToServer(false, imt.Meta.Server)	
			}
		}()
		go func(){//post
			indexstr := strconv.Itoa(index-2)
			if index==0 || index==1 || len(imt.Meta.Nodes)==0 {//the first two data index, or the  last node 
				postBlock(imt,chs_post[index] )
			}else{
				nextnode := imt.Meta.Net + `.` + imt.Meta.Nodes[0]
				endpoint := nextnode + `:` + imageConfig.Port 
				path := `/get_postState`
				url :=  `http://`+endpoint+path

				resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(indexstr))
				contents := make([]byte, 10)
				if err == nil && strings.HasPrefix(resp.Status, "200") {
					defer resp.Body.Close()
					n , _:= resp.Body.Read(contents)	
					if string(contents[:n])=="true"{
						err := postBlock(imt,chs_post[index] )
						if err!=nil{
							sendAckToServer(false, imt.Meta.Server)	
						}
					}
				}else{
					sendAckToServer(false, imt.Meta.Server)	
				}
			}
		}()
	}
	return 
}

func saveBlock(imt TransportUnit ,ch chan bool){

	if imt.Meta.DataIndex == 0{
		file, err := os.Create(`/tmp/`+imt.Meta.Filename)		
		if err != nil{
			util.PrintErr(err)
			sendAckToServer(false,imt.Meta.Server )
			ch <- false 
			return 
		}
		file.Close()
	}

	f, openerr := os.OpenFile(`/tmp/`+imt.Meta.Filename, os.O_RDWR,0666)	
	if openerr != nil{
		util.PrintErr(openerr)
		sendAckToServer(false,imt.Meta.Server )
		ch <- false 
		return 
	}
	_, seekerr := f.Seek(0,2)
	if seekerr != nil{
		util.PrintErr(seekerr)
		sendAckToServer(false,imt.Meta.Server )
		ch <- false 
		return 
	}
	
	length, writeErr := f.Write(imt.Body)
	if length!=len(imt.Body) || writeErr!=nil{
		util.PrintErr(writeErr)
		sendAckToServer(false,imt.Meta.Server )
		ch <- false 
		return 
	} 

	//if the block is the last one, send ack to server
	if imt.Meta.DataIndex == imt.Meta.BlockNum-1{
		if len(imt.Meta.Nodes)==0 {
			endpoint := imt.Meta.Server + ":" + imageConfig.Port
			url := `http://` + endpoint + `/transport_ack`
			if (*flagDebug){
				util.PrintErr("Post true to ", url)
			}
			resp, err := http.Post(url, util.POSTTYPE, strings.NewReader("true"))	
			if err!=nil || !strings.HasPrefix(resp.Status, "200"){
				util.PrintErr("Post true to ", url, "Failed")
				sendAckToServer(false,imt.Meta.Server )
			}
		}
	}

	if (*flagDebug){
		if imt.Meta.DataIndex==0{
			util.PrintErr("Saving block:")
		}
		util.Progress(int64(imt.Meta.DataIndex+1), int64(imt.Meta.BlockNum))
	}
	ch <- true
	defer f.Close()
	return 
}

/*
function:post block to next node, and set ch_post true
input : chs_pre_post[index-2]
	if true:post
	else  : blocked
*/
func postBlock(imt TransportUnit , ch_post chan bool)error{

	path :=`save_post` 

	if len(imt.Meta.Nodes)>=1{//not the last node
		nextnode := imt.Meta.Net + `.` + imt.Meta.Nodes[0]
		endpoint := nextnode + `:` + imageConfig.Port 
		url := `http://` + endpoint + `/`+ path

		imt.Meta.Nodes = imt.Meta.Nodes[1:]
		postBytes,_ := json.Marshal(imt) 

		resp,err := http.Post(url, util.POSTTYPE, strings.NewReader(string(postBytes)) )
		if err != nil || !strings.HasPrefix(resp.Status, "200"){
			ch_post <- false
			sendAckToServer(false,imt.Meta.Server )
			return errors.New("post data failed.")
		}else{
			defer resp.Body.Close()
		}
		if *flagDebug && DebugLevel>=2 {
			util.PrintErr(imt.Meta.DataIndex, "  data is posted.")
		}
	}
	ch_post <- true
	return nil
}
/*
function:
	post diff ack to server
*/
func sendAckToServer(ack bool, serverIp string)error{

	endpoint := serverIp +`:` +imageConfig.Port
	url := `http://` + endpoint + `transport_ack`
	var content string
	if ack{
		content="true"
	}else{
		content="false"
	}	
	_, err := http.Post(url, util.POSTTYPE, strings.NewReader(content ))	
	return err
}

/*
function:give content to chanel, according to the post data to TransportImageHandler
*/
func TransportAckHandler(w http.ResponseWriter,  r *http.Request){
	if clockClosed {
		//do nothing
		return 	
	}
	//only true and false is valid for the post data.	
	content := make([]byte, 1024)
	n, err := r.Body.Read(content)
	if err!=nil && err != io.EOF{
		http.Error(w, "bad request",400)
		util.PrintErr("Invalid input to TransportAckHandler")
		os.Exit(1)	
	}
	if n==4{//true
		clock <- true
		Process = true
		if *flagDebug{
			util.PrintErr("The ack info is true")
		}
	}else if n==5{//false
		clock <- false 
		Process = false 
		if *flagDebug{
			util.PrintErr("The ack info is false")
		}
	}else{
		Process = false 
		http.Error(w, "bad request",400)
		util.PrintErr("Invalid input to TransportAckHandler")
		if *flagDebug{
			util.PrintErr("Invalid input to TransportAckHandler")
		}
		os.Exit(1)	
	}
	clockClosed = true
	return 
}

func configInit()error{

	clock = make(chan bool , 1)

	err := imageConfig.Init()
	return err
}

/*
function:Save image to the tar file
input: image name or id, tar file name
*/
type Image2Tar struct{
	Image string
	TarFileName string
}

/*
Gare: the name of the image and tar file should be less than 200 letter
*/
func (i *Image2Tar)GetDataFromHttpReq(r *http.Request)error{
	contents := make([]byte, 500)	
	n, err := r.Body.Read(contents)
	if err!=nil && err!= io.EOF{
		return err	
	}
	defer r.Body.Close()
	jsonerr := json.Unmarshal(contents[:n], i)
	return jsonerr

}

/*
response:
400: bad request
500: can not build the command 'docker save'
501: can not run 'docker save'
*/
func SaveImageHandler(w http.ResponseWriter,  r *http.Request){
	if *flagDebug{
		util.PrintErr("[ SaveImageHandler ]")
	}
	var response util.HttpResponse
	var image2tar Image2Tar
	err := image2tar.GetDataFromHttpReq(r)
	if err!=nil{
		http.Error(w, "invalid input:image and tar", 400)
		response.Set(util.SERVER_ERROR, err.Error())	
		io.WriteString(w, response.String())
		return 
	}
	
	cmd := exec.Command("docker", "save", "-o", `/tmp/`+image2tar.TarFileName, image2tar.Image)
	if cmd==nil{
		http.Error(w, `can not build the command 'docker save '`, 500)
		response.Set(util.SERVER_ERROR, `Error: can not create command "docker save"`)	
		io.WriteString(w, response.String())
		return 
	}
	runerr := cmd.Run()
	if runerr != nil{
		http.Error(w, `can not run the command 'docker save '`, 500)
		response.Set(util.SERVER_ERROR, `Error: can not run command "docker save"`)	
		io.WriteString(w, response.String())
		return 
	}
	response.Set(util.OK, "Image "+image2tar.Image+ " has been saved.")
	io.WriteString(w, response.String())
	return 
}

/*
response:
400: bad request
500: server error
*/
func LoadImageHandler(w http.ResponseWriter,  r *http.Request){
	if *flagDebug{
		util.PrintErr("[ LoadImageHandler ]")	
	}
	var response util.HttpResponse
	var imageFullName string
	contents := make([]byte, 200)
	n, err := r.Body.Read(contents)
	if err != nil && err!=io.EOF{
		http.Error(w, "bad request", 400)
		response.Set(util.SERVER_ERROR, `Error: can not read imageFullName content from http.Request`)	
		io.WriteString(w, response.String())
		return 
	}
	imageFullName = `/tmp/` + string(contents[:n])

	cmd := exec.Command("docker", "load", "-i", imageFullName)
	if cmd==nil{
		http.Error(w, "server error", 500)
		response.Set(util.SERVER_ERROR, `Error: can not read imageFullName content from http.Request`)	
		response.Set(util.SERVER_ERROR, `Error: can not create command "docker load"`)	
		io.WriteString(w, response.String())
		return 
	}
	runerr := cmd.Run()
	if runerr != nil{
		http.Error(w, "server error", 500)
		response.Set(util.SERVER_ERROR, `Error: can not run command "docker load"`)	
		io.WriteString(w, response.String())
		return 
	}
	response.Set(util.OK, "Image "+imageFullName + " has been loaded.")
	io.WriteString(w, response.String())
	return 
}

/*
response:
400: bad request
500: server error
*/
func RmTarfileHandler(w http.ResponseWriter,  r *http.Request){
	if *flagDebug{
		util.PrintErr("[ RmTarfileHandler ]")	
	}
	var response util.HttpResponse
	var imageFullName string
	contents := make([]byte, 200)
	n, err := r.Body.Read(contents)
	if err != nil && err!=io.EOF{
		http.Error(w, "bad request", 400)
		response.Set(util.SERVER_ERROR, `Error: can not read imageFullName content from http.Request`)	
		io.WriteString(w, response.String())
		return 
	}
	imageFullName = `/tmp/`+string(contents[:n])
	rmerr := os.Remove(imageFullName)	
	if rmerr!=nil{
		http.Error(w, "bad request", 400)
		response.Set(util.SERVER_ERROR, "Can not remove "+imageFullName)	
	}else{
		response.Set(util.OK,imageFullName+ " has been removed.")	
	}
	io.WriteString(w, response.String())
	return 
}

/*
response:
400: bad request
500: server error
*/
func GetPostStateHandler(w http.ResponseWriter,  r *http.Request){
	if *flagDebug{
		//util.PrintErr("[", r.RemoteAddr,"GetPostStateHandler ]")	
	}
	contents := make([]byte, 200)
	n, err := r.Body.Read(contents)
	defer r.Body.Close()
	if err != nil && err!=io.EOF{
		http.Error(w, "bad request", 400)
		return 
	}else{
		postindex, _ := strconv.Atoi(string(contents[:n]))
		var ans string
		if postindex<0{
			ans="true"
		}else{
			value := <- chs_post[postindex]
			if value{
				ans="true"	
			}else{
				ans="false"	
			}
		}
		io.WriteString(w, ans)
	}
	return 
}
