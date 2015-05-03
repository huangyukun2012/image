package  main 

import(
	"flag"
	"log"
	"fmt"
	"wharf/util"
	"net/http"
)

var flagDebug *bool
var DebugLevel int
var Process bool
var chs_save []chan bool
var chs_post []chan bool

var clock chan bool	
var clockClosed bool

const (
	MAX_SIZE=2100
)

func main(){
	DebugLevel=0

	//fileBuffer = make([]byte, 100*BLOCKSIZE)

	flagd := flag.Bool("d", false, "run the resource as a daemon")	
	flagDebug = flag.Bool("D", false, "output the debuf info")	
	flag.Parse()	
	chs_save = make([]chan bool, MAX_SIZE)	
	chs_post= make([]chan bool, MAX_SIZE)	
	Process=true
	if *flagd {
		util.Daemon(0,1)
	}
	errinit := configInit()
	if errinit != nil{
		util.PrintErr("config file for image can not be read.")	
	}
	http.HandleFunc("/transport_image", TransportImageHandler)
	http.HandleFunc("/save_post",SaveAndPostHandler)
	http.HandleFunc("/transport_ack",TransportAckHandler)

	http.HandleFunc("/save_image", SaveImageHandler)
	http.HandleFunc("/load_image",LoadImageHandler)
	http.HandleFunc("/rm_tarfile",RmTarfileHandler)
	http.HandleFunc("/get_postState",GetPostStateHandler)
	
	if *flagDebug{
		fmt.Println("Listening for image...")
	}
	errhttp := http.ListenAndServe(":"+imageConfig.Port, nil)
	if errhttp != nil{
		log.Fatal("InitServer: ListenAndServe ", errhttp)	
	}
}
