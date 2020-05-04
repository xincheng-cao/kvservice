package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type Triplet struct{
	Key string
	Value string
	Op string
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currView viewservice.View
	database map[string]string //k/v
	requestList map[int64]Triplet
	syncReq bool
}

func (pb *PBServer) P2BGet(args *GetArgs, reply *GetReply) error {
	var currBackup string
	currBackup=pb.currView.Backup
	if currBackup==pb.me{
		//check request list
		var key string
		var id int64
		key=args.Key
		id=args.Id
		var tempRequest Triplet
		var dup bool
		tempRequest,dup=pb.requestList[id]
		var val string
		var exist bool
		val, exist=pb.database[key]

		if tempRequest.Key==key && dup{
			reply.Value=pb.database[key]
			reply.Err=OK
			return nil
		}else{
			if exist{
				reply.Value=val
				reply.Err=OK
			}else{
				reply.Err=ErrNoKey
			}
			var request *Triplet
			request=new(Triplet)
			request.Op="Get"
			request.Key=key
			request.Value=""
			pb.requestList[id]=*request
		}
	}else{
		reply.Err=ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock() //defer statement defers the execution of a function until the surrounding function returns.

	var currPrimary string
	currPrimary=pb.currView.Primary
	var currBackup string
	currBackup=pb.currView.Backup
	if currPrimary==pb.me{
		//check request list
		var key string
		var id int64
		key=args.Key
		id=args.Id
		var tempRequest Triplet
		var dup bool
		tempRequest,dup=pb.requestList[id]
		var val string
		var exist bool
		val, exist=pb.database[key]

		if tempRequest.Key==key && dup{
			reply.Value=val
			reply.Err=OK
		}else{
			if exist{
				reply.Value=val
				reply.Err=OK
			}else{
				reply.Err=ErrNoKey
			}
			var request *Triplet
			request=new(Triplet)
			request.Op="Get"
			request.Key=key
			request.Value=""
			pb.requestList[id]=*request
			if currBackup==""{
				return nil
			}else{
				var success bool
				success=call(currBackup,"PBServer.P2BGet",args,reply)
				if !success || reply.Err!=OK || reply.Value != pb.database[key]{
					pb.syncReq=true
				}
			}
		}
	}else{
		reply.Err=ErrWrongServer
	}

	return nil
}

func (pb *PBServer) P2BPutAppend(args *PutAppendArgs, reply *PutAppendReply) error{
	var currBackup string
	currBackup=pb.currView.Backup
	if currBackup==pb.me{
		var key string
		var id int64
		var val string
		var op string
		key=args.Key
		val=args.Value
		op=args.Op
		id=args.Id
		var tempRequest Triplet
		var dup bool
		tempRequest,dup=pb.requestList[id]
		if dup&&key==tempRequest.Key&&val==tempRequest.Value&&op==tempRequest.Op{
			reply.Err=OK
		}else{
			if op=="Put"{
				pb.database[key]=val
			}
			if op=="Append" {
				pb.database[key]+=val
			}
			var request *Triplet
			request=new(Triplet)
			request.Op=op
			request.Key=key
			request.Value=val
			pb.requestList[id]=*request
			reply.Err=OK
		}
	}else{
		reply.Err=ErrWrongServer
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var currPrimary string
	currPrimary=pb.currView.Primary
	var currBackup string
	currBackup=pb.currView.Backup
	if currPrimary==pb.me{
		var key string
		var id int64
		var val string
		var op string
		key=args.Key
		val=args.Value
		op=args.Op
		id=args.Id
		var tempRequest Triplet
		var dup bool
		tempRequest,dup=pb.requestList[id]
		if dup&&key==tempRequest.Key&&val==tempRequest.Value&&op==tempRequest.Op{
			reply.Err=OK
		}else{
			if op=="Put"{
				pb.database[key]=val
			}
			if op=="Append" {
				pb.database[key]+=val
			}
			var request *Triplet
			request=new(Triplet)
			request.Op=op
			request.Key=key
			request.Value=val
			pb.requestList[id]=*request
			reply.Err=OK
			if currBackup==""{
				return nil
			}else{
				var success bool
				success=call(currBackup,"PBServer.P2BPutAppend",args,reply)
				if !success||reply.Err!=OK||pb.database[key]!=val{
					pb.syncReq=true
				}
			}
		}
	}else{
		reply.Err=ErrWrongServer
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) P2BSync(args *P2BArgs, reply *P2BReply) error {
	var viewnum uint
	var newView viewservice.View
	viewnum=pb.currView.Viewnum
	newView,_=pb.vs.Ping(viewnum)
	var newBackup string
	newBackup=newView.Backup
	if newBackup==pb.me {
		pb.database=args.Database
		pb.requestList=args.RequestList
	}else {
		reply.Err=ErrWrongServer
	}
	return nil
}

func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()

	var viewnum uint
	var newView viewservice.View
	viewnum=pb.currView.Viewnum
	newView,_=pb.vs.Ping(viewnum)
	var newPrimary string
	newPrimary=newView.Primary
	var newBackup string
	newBackup=newView.Backup
	var currBackup string
	currBackup=pb.currView.Backup
	if newPrimary==pb.me&&newBackup!=""&&newBackup!=currBackup{
		pb.syncReq=true
	}
	if pb.syncReq==true {
		var args *P2BArgs
		var	reply *P2BReply
		args=new(P2BArgs)
		args.Database=pb.database
		args.RequestList=pb.requestList
		reply=new(P2BReply)
		reply.Err=""
		var success bool
		success=call(newBackup,"PBServer.P2BSync",args,reply)
		pb.syncReq=false
		if !success||reply.Err!=OK{
			pb.syncReq=true
		}
	}
	pb.currView=newView
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}


func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.currView=*new(viewservice.View)
	pb.currView.Viewnum=0
	pb.currView.Primary=""
	pb.currView.Backup=""
	pb.database=make(map[string]string)
	pb.requestList=make(map[int64]Triplet)
	pb.syncReq=false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
