package master

import (
	"encoding/json"
	"github.com/anakin/crontab/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

type ApiServer struct {
	httpServer *http.Server
}

var (
	G_apiServer *ApiServer
)

func handleJobDelete(resp http.ResponseWriter, req *http.Request) {

	var (
		err    error
		name   string
		oldJob *common.Job
		bytes  []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	name = req.PostForm.Get("name")
	if oldJob, err = G_jobMgr.DeleteJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildRessponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}

ERR:
	if bytes, err = common.BuildRessponse(-1, "fail", ""); err == nil {
		resp.Write(bytes)
	}
}

func handleJobList(resp http.ResponseWriter, req *http.Request) {
	var (
		jobList []*common.Job
		bytes   []byte
		err     error
	)

	if jobList, err = G_jobMgr.ListJobs(); err != nil {
		if bytes, err = common.BuildRessponse(-1, "fail list jobs", nil); err == nil {
			resp.Write(bytes)
		}
	} else {
		if bytes, err = common.BuildRessponse(0, "successsss", jobList); err == nil {
			resp.Write(bytes)
		}
	}
}

func handleJobKill(resp http.ResponseWriter, req *http.Request) {

	var (
		err   error
		name  string
		bytes []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}
	name = req.PostForm.Get("name")
	if err = G_jobMgr.KillJob(name); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildRessponse(0, "success", nil); err == nil {
		resp.Write(bytes)
	}

ERR:
	if bytes, err = common.BuildRessponse(-1, err.Error(), nil); err == nil {
		resp.Write(bytes)
	}
}

func handleJobSave(resp http.ResponseWriter, req *http.Request) {
	var (
		err     error
		postJob string
		job     common.Job
		oldJob  *common.Job
		bytes   []byte
	)
	if err = req.ParseForm(); err != nil {
		goto ERR
	}

	postJob = req.PostForm.Get("job")
	if err = json.Unmarshal([]byte(postJob), &job); err != nil {
		goto ERR
	}

	if oldJob, err = G_jobMgr.SaveJob(&job); err != nil {
		goto ERR
	}
	if bytes, err = common.BuildRessponse(0, "success", oldJob); err == nil {
		resp.Write(bytes)
	}
	return

ERR:
	if bytes, err = common.BuildRessponse(-1, err.Error(), ""); err == nil {
		resp.Write(bytes)
	}
}

func InitApiServer() (err error) {
	var (
		mux           *http.ServeMux
		listener      net.Listener
		httpServer    *http.Server
		staticDir     http.Dir
		staticHandler http.Handler
	)
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/delete", handleJobDelete)

	staticDir = http.Dir("./webroot")
	staticHandler = http.FileServer(staticDir)
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	if listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort)); err != nil {
		return
	}

	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}
	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}
	go httpServer.Serve(listener)

	for {
		time.Sleep(1 * time.Second)
	}
	return
}
