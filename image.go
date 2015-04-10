package  main 

import(
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
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
	Nodes []string `1.1`
	Server	string `ip`
}

func (i *ImageTransportHead)GetDataFromHttpReqest( r *http.Request) error{
	content := make([]byte, BLOCKSIZE)
	n, err := r.Body.Read(content)	
	
	if err == io.EOF{
		if *flagDebug{
			util.PrintErr("encount io.EOF when reading r.Body.")
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
	defer r.Body.Close()

	if *flagDebug{
		util.PrintErr("content length is", r.ContentLength)	
	}

	content := make([]byte, r.ContentLength+1)

	var addition  int
	var readlen int
	addition, err := r.Body.Read(content)	
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
	if *flagDebug{
		util.PrintErr("read", readlen , "in total")
	}
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


var clock chan bool	
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

*/
func TransportImageHandler( w http.ResponseWriter, r *http.Request){
	var response ImageTransportResponse
	var imt ImageTransportHead
	err := imt.GetDataFromHttpReqest( r )
	if err!=nil{
		response.Set(util.SERVER_ERROR, err.Error())	
		return 
	}

	if len(imt.Nodes)<1{
		response.Set(util.SERVER_ERROR,"no destination.")
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
	var endOfFile bool
	endOfFile = false 
	var numOfBlock int
	imt.DataIndex=-1
	for ; !endOfFile ;{//every block
		buf := make([]byte,BLOCKSIZE)
		n , readerr := f.Read(buf)
		if n<BLOCKSIZE || readerr == io.EOF{
			endOfFile=true
			imt.DataIndex=-1
		}else{
			if *flagDebug {
				util.PrintErr("read file len:", n)
			}
			imt.DataIndex++
		}
		numOfBlock++
		var postData TransportUnit
		postData.Init(imt)
		postData.Body = buf[:n]

		postBytes, jsonerr := json.Marshal(postData)
		if jsonerr != nil{
			util.PrintErr("In TransportImageHandler:we can not marshal the post data")	
			response.Set(util.SERVER_ERROR, jsonerr.Error())
			return 
		}
		if *flagDebug {
			util.PrintErr("json marshal  len:", len(string(postBytes)))
		}
		res , reserr := http.Post(url, util.POSTTYPE, strings.NewReader(string(postBytes)))
		if reserr!= nil{//post failed
			util.PrintErr(reserr.Error())	
			response.Set(util.SERVER_ERROR, reserr.Error())
			return 
		}
		if strings.HasPrefix(res.Status, "200"){//post OK
			//do nothing
		}else{//post failed
			util.PrintErr(res.Status)	
			response.Set(util.SERVER_ERROR, res.Status)
			return 
		}

	}
	//wait for the answer from the last node
	bindwidth := 10
	duration := time.Duration( (1.0*numOfBlock/bindwidth+ (len(imt.Nodes)+1)/bindwidth )*5)*time.Second	
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
	var response ImageTransportResponse
	var imt TransportUnit 
	err := imt.GetDataFromHttpReqest( r )

	if err!=nil{
		util.PrintErr(err)
		response.Set(util.SERVER_ERROR, err.Error())	
		io.WriteString(w, response.String())
		sendAckToServer(false,imt.Meta.Server )
		return 
	}
	
	path :=`save_post` 
	if imt.Meta.DataIndex == 0{
		file, err := os.Create(`/tmp/`+imt.Meta.Filename)		
		if err != nil{
			util.PrintErr(err)
			response.Set(util.SERVER_ERROR, err.Error())	
			io.WriteString(w, response.String())
			sendAckToServer(false,imt.Meta.Server )
			return 
		}
		file.Close()
	}

	
	f, openerr := os.OpenFile(`/tmp/`+imt.Meta.Filename, os.O_RDWR,0666)	
	if openerr != nil{
		util.PrintErr(err)
		response.Set(util.SERVER_ERROR, "We can not found the image in the server.")	
		io.WriteString(w, response.String())
		sendAckToServer(false,imt.Meta.Server )
		return 
	}
	_, seekerr := f.Seek(0,2)
	if seekerr != nil{
		util.PrintErr(err)
		response.Set(util.SERVER_ERROR, "seek failed.")	
		io.WriteString(w, response.String())
		sendAckToServer(false,imt.Meta.Server )
		return 
	}
	
	length, writeErr := f.Write(imt.Body)
	if length!=len(imt.Body) || writeErr!=nil{
		util.PrintErr(writeErr)
		response.Set(util.SERVER_ERROR, "write failed.")	
		io.WriteString(w, response.String())
		sendAckToServer(false,imt.Meta.Server )
		return 
	} 
	if (*flagDebug){
		util.PrintErr("one block is saved.")
	}
	defer f.Close()
	//above: save end.

	if len(imt.Meta.Nodes)<1{//the last node
		if imt.Meta.DataIndex == -1{//the last block
			endpoint := imt.Meta.Server + ":" + imageConfig.Port
			url := `http://` + endpoint + `/transport_ack`
			if (*flagDebug){
				util.PrintErr("Post true to ", url)
			}
			resp, err := http.Post(url, util.POSTTYPE, strings.NewReader("true"))	
			if err!=nil || !strings.HasPrefix(resp.Status, "200"){
				util.PrintErr("Post true to ", url, "Failed")
				response.Set(util.SERVER_ERROR, "can not post data to server.")	
				io.WriteString(w, response.String())
				sendAckToServer(false,imt.Meta.Server )
				return 
			}
		   response.Set(util.OK,"this file is transported end.")	
		}else{
		   response.Set(util.OK,"this block is transported end.")	
			io.WriteString(w, response.String())
		   return 
		}	
	}else{//not the last node
		nextnode := imt.Meta.Net + `.` + imt.Meta.Nodes[0]
		endpoint := nextnode + `:` + imageConfig.Port 
		url := `http://` + endpoint + `/`+ path

		imt.Meta.Nodes = imt.Meta.Nodes[1:]
		postBytes,_ := json.Marshal(imt) 
		resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(postBytes)) )
		if err != nil || !strings.HasPrefix(resp.Status, "200"){
			response.Set(util.SERVER_ERROR, "can not post data to server.")	
			io.WriteString(w, response.String())
			sendAckToServer(false,imt.Meta.Server )
			return 
		}
		response.Set(util.OK, "This block is posted to next node.")
	}
	if *flagDebug{
		util.PrintErr("The data is posted.")
	}
	io.WriteString(w, response.String())				
	return 

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
	//only true and false is valid for the post data.	
	content := make([]byte, 1024)
	n, err := r.Body.Read(content)
	if err!=nil && err != io.EOF{
		util.PrintErr("Invalid input to TransportAckHandler")
		os.Exit(1)	
	}
	if n==4{//true
		clock <- true
		if *flagDebug{
			util.PrintErr("The ack info is true")
		}
	}else if n==5{//false
		if *flagDebug{
			util.PrintErr("The ack info is false")
		}
		clock <- false 
	}else{
		if *flagDebug{
			util.PrintErr("Invalid input to TransportAckHandler")
		}
		os.Exit(1)	
	}
	if *flagDebug{
		util.PrintErr("ack ended.")
	}
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

func SaveImageHandler(w http.ResponseWriter,  r *http.Request){
	var response util.HttpResponse
	var image2tar Image2Tar
	err := image2tar.GetDataFromHttpReq(r)
	if err!=nil{
		response.Set(util.SERVER_ERROR, err.Error())	
		io.WriteString(w, response.String())
		return 
	}
	
	cmd := exec.Command("docker", "save", "-o", `/tmp/`+image2tar.TarFileName, image2tar.Image)
	if cmd==nil{
		response.Set(util.SERVER_ERROR, `Error: can not create command "docker save"`)	
		io.WriteString(w, response.String())
		return 
	}
	runerr := cmd.Run()
	if runerr != nil{
		response.Set(util.SERVER_ERROR, `Error: can not run command "docker save"`)	
		io.WriteString(w, response.String())
		return 
	}
	response.Set(util.OK, "Image "+image2tar.Image+ " has been saved.")
	io.WriteString(w, response.String())
	return 
}

func LoadImageHandler(w http.ResponseWriter,  r *http.Request){
	var response util.HttpResponse
	var imageFullName string
	contents := make([]byte, 200)
	n, err := r.Body.Read(contents)
	if err != nil && err!=io.EOF{
		response.Set(util.SERVER_ERROR, `Error: can not read imageFullName content from http.Request`)	
		io.WriteString(w, response.String())
		return 
	}
	imageFullName = `/tmp/` + string(contents[:n])

	cmd := exec.Command("docker", "load", "-i", imageFullName)
	if cmd==nil{
		response.Set(util.SERVER_ERROR, `Error: can not create command "docker load"`)	
		io.WriteString(w, response.String())
		return 
	}
	runerr := cmd.Run()
	if runerr != nil{
		response.Set(util.SERVER_ERROR, `Error: can not run command "docker load"`)	
		io.WriteString(w, response.String())
		return 
	}
	response.Set(util.OK, "Image "+imageFullName + " has been loaded.")
	io.WriteString(w, response.String())
	return 
}

func RmTarfileHandler(w http.ResponseWriter,  r *http.Request){
	var response util.HttpResponse
	var imageFullName string
	contents := make([]byte, 200)
	n, err := r.Body.Read(contents)
	if err != nil && err!=io.EOF{
		response.Set(util.SERVER_ERROR, `Error: can not read imageFullName content from http.Request`)	
		io.WriteString(w, response.String())
		return 
	}
	imageFullName = `/tmp/`+string(contents[:n])
	rmerr := os.Remove(imageFullName)	
	if rmerr!=nil{
		response.Set(util.SERVER_ERROR, "Can not remove "+imageFullName)	
	}else{
		response.Set(util.OK,imageFullName+ " has been removed.")	
	}
	io.WriteString(w, response.String())
	return 
}
