package  main 

import(
	"flag"
	"log"
	"wharf/util"
	"net/http"
)
var flagDebug *bool


func main(){
	
	flagd := flag.Bool("d", false, "run the resource as a daemon")	
	flagDebug = flag.Bool("D", false, "output the debuf info")	
	flag.Parse()	

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

	errhttp := http.ListenAndServe(":"+imageConfig.Port, nil)
	if errhttp != nil{
		log.Fatal("InitServer: ListenAndServe ", errhttp)	
	}
}
