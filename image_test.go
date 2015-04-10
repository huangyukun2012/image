package main 

import (
	"strings"
	"net/http"
	"fmt"
	/* "encoding/json" */
	"testing"
	"wharf/util"
)

/*
func  Test_transport(t *testing.T){

	var info ImageTransportHead	
	info =ImageTransportHead{Net:"192.168", Filename: "test.img", 
										Nodes:[]string{"122.10"}, 
										DataIndex:0, 
										Server:"192.168.122.1"}
	data , _:= json.Marshal(info)
	url := `http://`+"192.168.122.1" + `:` +`7000`+`/transport_image`
	resp, err := http.Post(url, util.POSTTYPE, strings.NewReader(string(data)))
	if err != nil{
		t.Error(err.Error())
	}else{
		var content []byte
		content = make([]byte, 200)
		resp.Body.Read(content)
		fmt.Println(string(content))	
	}

	return 
}

func  Test_SaveImage(t *testing.T){

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
}

func  Test_LoadImage(t *testing.T){

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
}
*/
func  Test_RmImage(t *testing.T){

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
}
