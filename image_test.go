package main 

import (
//	"strconv"
	"strings"
	"net/http"
	"fmt"
//	"time"
//	"os"
//	"sort"
	"encoding/json"
	"testing"
	"wharf/util"
)

func  Test_transport(t *testing.T){

	var info ImageTransportHead	
	info =ImageTransportHead{Net:"10.18", Filename: "256.img", 
//									Nodes:[]string{"111.2"},
//										Nodes:[]string{"111.2","111.3"},
//										Nodes:[]string{"111.2","111.3","111.4","111.5",},
										Nodes:[]string{"111.3","111.4"},
//										Nodes:[]string{"111.2","111.3","111.4","111.5","111.6","111.7","111.8","111.9"}, 
//										Nodes:[]string{"111.2","111.3","111.4","111.5","111.6","111.7","111.8","111.9",\
//													"111.10","111.11","111.12",111.13",111.14","111.15", "111.16","111.17"}, 
										DataIndex:0, 
										Server:"10.18.111.1"}
	data , _:= json.Marshal(info)
	url := `http://`+"10.18.111.1" + `:` +`7000`+`/transport_image`
	resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(data)))
	if err != nil{
		t.Error(err.Error())
	}else{
		fmt.Println(resp.Status)
		var content []byte
		content = make([]byte, 200)
		resp.Body.Read(content)
		fmt.Println(string(content))	
	}

	return 
}

func  Test_SaveImage(t *testing.T){
/*
	var info Image2Tar 
	info =Image2Tar{Image:"ubuntu:latest", TarFileName: `/tmp/ubuntu.img`}
	data , _:= json.Marshal(info)
	var url string
	 url = `http://`+"192.168.122.1" + `:` +`7000`+`/save_image` 
	resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(data)))
	if err != nil || !strings.HasPrefix(resp.Status,"200"){
		t.Error(err.Error())
	}else{
		var content []byte
		content = make([]byte, 200)
		resp.Body.Read(content)
		fmt.Println(string(content))	
	}
	return 
	*/
}

func  Test_LoadImage(t *testing.T){
/*
	data := `/tmp/ubuntu.img`
	var url string
	url = `http://`+"192.168.122.1" + `:` +`7000`+`/load_image`
	resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(data)))
	if err != nil || !strings.HasPrefix(resp.Status,"200"){
		t.Error(err.Error())
	}else{
		var content []byte
		content = make([]byte, 200)
		resp.Body.Read(content)
		fmt.Println(string(content))	
	}
	return 
	*/
}
func  Test_RmImage(t *testing.T){
	/*
	data := `/tmp/ubuntu.img`
	var url string
	url = `http://`+"192.168.122.1" + `:` +`7000`+`/rm_tarfile`
	resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(data)))
	if err != nil || !strings.HasPrefix(resp.Status,"200"){
		t.Error(err.Error())
	}else{
		var content []byte
		content = make([]byte, 200)
		resp.Body.Read(content)
		fmt.Println(string(content))	
	}
	return 
	fmt.Fprintf(os.Stdout,"\b\b\b\b%s%%","abc")
	os.Stdout.Sync()
	time.Sleep(time.Second)
	fmt.Fprintf(os.Stdout,"\b\b\b\b%s%%","123")
	Progress(1,100)
	time.Sleep(time.Second)
	time.Sleep(time.Second)
	Progress(2,100)
	*/
}

